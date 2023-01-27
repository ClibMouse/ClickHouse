#!/usr/bin/env python3
import argparse
import json
import logging
import os
import time
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from github import Github

from clickhouse_helper import ClickHouseHelper, prepare_tests_results_for_clickhouse
from commit_status_helper import format_description, get_commit, post_commit_status
from env_helper import (
    GITHUB_WORKSPACE,
    ROOT_DIR,
    RUNNER_TEMP,
    GITHUB_RUN_URL,
    DOCKER_USER,
    DOCKER_REPO,
)
from get_robot_token import get_best_robot_token, get_parameter_from_ssm
from pr_info import PRInfo
from report import TestResults, TestResult
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen
from upload_result_helper import upload_results
from docker_images_helper import DockerImageData, docker_login, get_images_oredered_list

NAME = "Push to Dockerhub"
TEMP_PATH = Path(RUNNER_TEMP) / "docker_images_check"
TEMP_PATH.mkdir(parents=True, exist_ok=True)


class DockerImage:
    def __init__(
        self,
        path: str,
        repo: str,
        only_amd64: bool,
        parent: Optional["DockerImage"] = None,
        gh_repo: str = REPO_COPY,
    ):
        assert not path.startswith("/")
        self.path = path
        self.full_path = Path(gh_repo) / path
        self.repo = repo
        self.only_amd64 = only_amd64
        self.parent = parent
        self.built = False

    def __eq__(self, other) -> bool:  # type: ignore
        """Is used to check if DockerImage is in a set or not"""
        return (
            self.path == other.path
            and self.repo == self.repo
            and self.only_amd64 == other.only_amd64
        )

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, DockerImage):
            return False
        if self.parent and not other.parent:
            return False
        if not self.parent and other.parent:
            return True
        if self.path < other.path:
            return True
        if self.repo < other.repo:
            return True
        return False

    def __hash__(self):
        return hash(self.path)

    def __str__(self):
        return self.repo

    def __repr__(self):
        return f"DockerImage(path={self.path},repo={self.repo},parent={self.parent})"


def get_changed_docker_images(
    pr_info: PRInfo, images_dict: ImagesDict, docker_repo: str
) -> Set[DockerImage]:
    if not images_dict:
        return set()

    files_changed = pr_info.changed_files

    logging.info(
        "Changed files for PR %s @ %s: %s",
        pr_info.number,
        pr_info.sha,
        str(files_changed),
    )

    changed_images = []

    for dockerfile_dir, image_description in images_dict.items():
        for f in files_changed:
            if f.startswith(dockerfile_dir):
                name = image_description["name"]
                only_amd64 = image_description.get("only_amd64", False)
                logging.info(
                    "Found changed file '%s' which affects "
                    "docker image '%s' with path '%s'",
                    f,
                    name,
                    dockerfile_dir,
                )
                changed_images.append(DockerImage(dockerfile_dir, name, only_amd64))
                break

    # The order is important: dependents should go later than bases, so that
    # they are built with updated base versions.
    index = 0
    while index < len(changed_images):
        image = changed_images[index]
        for dependent in images_dict[image.path]["dependent"]:
            logging.info(
                "Marking docker image '%s' as changed because it "
                "depends on changed docker image '%s'",
                dependent,
                image,
            )
            name = docker_repo + "/" + images_dict[dependent]["name"]
            only_amd64 = images_dict[dependent].get("only_amd64", False)
            changed_images.append(DockerImage(dependent, name, only_amd64, image))
        index += 1
        if index > 5 * len(images_dict):
            # Sanity check to prevent infinite loop.
            raise RuntimeError(
                f"Too many changed docker images, this is a bug. {changed_images}"
            )

    # With reversed changed_images set will use images with parents first, and
    # images without parents then
    result = set(reversed(changed_images))
    logging.info(
        "Changed docker images for PR %s @ %s: '%s'",
        pr_info.number,
        pr_info.sha,
        result,
    )
    return result


def gen_versions(
    pr_info: PRInfo, suffix: Optional[str]
) -> Tuple[List[str], Union[str, List[str]]]:
    pr_commit_version = str(pr_info.number) + "-" + pr_info.sha
    # The order is important, PR number is used as cache during the build
    versions = [str(pr_info.number), pr_commit_version]
    result_version = pr_commit_version  # type: Union[str, List[str]]
    if pr_info.number == 0 and pr_info.base_ref == "master":
        # First get the latest for cache
        versions.insert(0, "latest")

    if suffix:
        # We should build architecture specific images separately and merge a
        # manifest lately in a different script
        versions = [f"{v}-{suffix}" for v in versions]
        # changed_images_{suffix}.json should contain all changed images
        result_version = versions

    return versions, result_version


def build_and_push_dummy_image(
    image: DockerImage,
    version_string: str,
    push: bool,
) -> Tuple[bool, Path]:
    dummy_source = "ubuntu:20.04"
    logging.info("Building docker image %s as %s", image.repo, dummy_source)
    build_log = (
        Path(TEMP_PATH)
        / f"build_and_push_log_{image.repo.replace('/', '_')}_{version_string}.log"
    )
    cmd = (
        f"docker pull {dummy_source}; "
        f"docker tag {dummy_source} {image.repo}:{version_string}; "
    )
    if push:
        cmd += f"docker push {image.repo}:{version_string}"

    logging.info("Docker command to run: %s", cmd)
    with TeePopen(cmd, build_log) as proc:
        retcode = proc.wait()

    if retcode != 0:
        return False, build_log

    logging.info("Processing of %s successfully finished", image.repo)
    return True, build_log


def build_and_push_one_image(
    image: DockerImageData,
    version_string: str,
    additional_cache: List[str],
    push: bool,
    from_tag: Optional[str] = None,
) -> Tuple[bool, Path]:
    logging.info(
        "Building docker image %s with version %s from path %s",
        image.repo,
        version_string,
        image.path,
    )
    build_log = (
        Path(TEMP_PATH)
        / f"build_and_push_log_{image.repo.replace('/', '_')}_{version_string}.log"
    )
    push_arg = ""
    if push:
        push_arg = "--push "

    from_tag_arg = ""
    if from_tag:
        from_tag_arg = f"--build-arg FROM_TAG={from_tag} "

    cache_from = (
        f"--cache-from type=registry,ref={image.repo}:{version_string} "
        f"--cache-from type=registry,ref={image.repo}:latest"
    )
    for tag in additional_cache:
        assert tag
        cache_from = f"{cache_from} --cache-from type=registry,ref={image.repo}:{tag}"

    cmd = (
        "docker buildx build --builder default "
        f"--label build-url={GITHUB_RUN_URL} "
        f"{from_tag_arg}"
        f"--build-arg DOCKER_REPO={DOCKER_REPO} "
        # A hack to invalidate cache, grep for it in docker/ dir
        f"--build-arg CACHE_INVALIDATOR={GITHUB_RUN_URL} "
        f"--tag {image.repo}:{version_string} "
        f"{cache_from} "
        f"--cache-to type=inline,mode=max "
        f"{push_arg}"
        f"--progress plain {image.path}"
    )
    logging.info("Docker command to run: %s", cmd)
    with TeePopen(cmd, build_log) as proc:
        retcode = proc.wait()

    if retcode != 0:
        return False, build_log

    logging.info("Processing of %s successfully finished", image.repo)
    return True, build_log


def process_single_image(
    image: DockerImageData,
    versions: List[str],
    additional_cache: List[str],
    push: bool,
    from_tag: Optional[str] = None,
) -> TestResults:
    logging.info("Image will be pushed with versions %s", ", ".join(versions))
    results = []  # type: TestResults
    for ver in versions:
        stopwatch = Stopwatch()
        for i in range(5):
            success, build_log = build_and_push_one_image(
                image, ver, additional_cache, push, from_tag
            )
            if success:
                results.append(
                    TestResult(
                        image.repo + ":" + ver,
                        "OK",
                        stopwatch.duration_seconds,
                        [build_log],
                    )
                )
                break
            logging.info(
                "Got error will retry %s time and sleep for %s seconds", i, i * 5
            )
            time.sleep(i * 5)
        else:
            results.append(
                TestResult(
                    image.repo + ":" + ver,
                    "FAIL",
                    stopwatch.duration_seconds,
                    [build_log],
                )
            )

    logging.info("Processing finished")
    image.built = True
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Program to build changed or given docker images with all "
        "dependant images. Example for local running: "
        "python docker_images_check.py --no-push-images --no-reports "
        "--image-path docker/packager/binary",
    )

    parser.add_argument("--suffix", type=str, required=True, help="arch suffix")
    parser.add_argument(
        "--missing-images",
        type=str,
        required=True,
        help="json string or json file with images to build {IMAGE: TAG} or type all to build all",
    )
    parser.add_argument(
        "--image-tags",
        type=str,
        required=True,
        help="json string or json file with all images and their tags {IMAGE: TAG}",
    )
    parser.add_argument("--reports", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-reports",
        action="store_false",
        dest="reports",
        default=argparse.SUPPRESS,
        help="don't push reports to S3 and github",
    )
    parser.add_argument("--push", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-push-images",
        action="store_false",
        dest="push",
        default=argparse.SUPPRESS,
        help="don't push images to docker hub",
    )
    return parser.parse_args()


def main():
    # to be always aligned with docker paths from image.json
    os.chdir(ROOT_DIR)
    logging.basicConfig(level=logging.INFO)
    stopwatch = Stopwatch()

    args = parse_args()
    if args.push:
        subprocess.check_output(  # pylint: disable=unexpected-keyword-arg
            "docker login {} --username '{}' --password-stdin".format(
                DOCKER_REPO, DOCKER_USER
            ),
            input=get_parameter_from_ssm("dockerhub_robot_password"),
            encoding="utf-8",
            shell=True,
        )

    images_dict = get_images_dict(Path(REPO_COPY), IMAGES_FILE_PATH)

    pr_info = PRInfo()
    if args.all:
        pr_info.changed_files = set(images_dict.keys())
    elif args.image_path:
        pr_info.changed_files = set(i for i in args.image_path)
    else:
        try:
            pr_info.fetch_changed_files()
        except TypeError:
            # If the event does not contain diff, nothing will be built
            pass

    changed_images = get_changed_docker_images(pr_info, images_dict, DOCKER_REPO)
    if changed_images:
        logging.info(
            "Has changed images: %s", ", ".join([im.path for im in changed_images])
        )

    image_versions, result_version = gen_versions(pr_info, args.suffix)

    result_images = {}
    test_results = []  # type: TestResults
    additional_cache = []  # type: List[str]
    # FIXME: add all tags taht we need. latest on master!
    # if pr_info.release_pr:
    #     logging.info("Use %s as additional cache tag", pr_info.release_pr)
    #     additional_cache.append(str(pr_info.release_pr))
    # if pr_info.merged_pr:
    #     logging.info("Use %s as additional cache tag", pr_info.merged_pr)
    #     additional_cache.append(str(pr_info.merged_pr))

    ok_cnt = 0
    status = "success"
    image_tags = (
        json.loads(args.image_tags)
        if not os.path.isfile(args.image_tags)
        else json.load(open(args.image_tags))
    )
    missing_images = (
        image_tags
        if args.missing_images == "all"
        else json.loads(args.missing_images)
        if not os.path.isfile(args.missing_images)
        else json.load(open(args.missing_images))
    )
    images_build_list = get_images_oredered_list()

    for image in images_build_list:
        if image.repo not in missing_images:
            continue
        logging.info("Start building image: %s", image)

        image_versions = (
            [image_tags[image.repo]]
            if not args.suffix
            else [f"{image_tags[image.repo]}-{args.suffix}"]
        )
        parent_version = (
            None
            if not image.parent
            else image_tags[image.parent]
            if not args.suffix
            else f"{image_tags[image.parent]}-{args.suffix}"
        )

        res = process_single_image(
            image,
            image_versions,
            additional_cache,
            args.push,
            from_tag=parent_version,
        )
        test_results += res
        if all(x.status == "OK" for x in res):
            ok_cnt += 1
        else:
            status = "failure"
            break  # No need to continue with next images

    description = format_description(
        f"Images build done. built {ok_cnt} out of {len(missing_images)} images."
    )

    s3_helper = S3Helper()

    pr_info = PRInfo()
    url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, [], NAME)

    print(f"::notice ::Report url: {url}")

    if not args.reports:
        return

    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)
    post_commit_status(
        commit, status, url, description, NAME, pr_info, dump_to_file=True
    )

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        test_results,
        status,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        url,
        NAME,
    )
    ch_helper = ClickHouseHelper()
    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)

    if status == "failure":
        sys.exit(1)


if __name__ == "__main__":
    main()
