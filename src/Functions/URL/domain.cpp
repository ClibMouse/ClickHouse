#include "domain.h"

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>

namespace DB
{

struct NameDomain { static constexpr auto name = "domain"; };
using FunctionDomain = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<false, 0>>, NameDomain>;

struct NameDomainRFC { static constexpr auto name = "domainRFC"; };
using FunctionDomainRFC = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<false, 1>>, NameDomainRFC>;

struct NameDomainKQL { static constexpr auto name = "domainKQL"; };
using FunctionDomainKQL = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<false, 2>>, NameDomainKQL>;

REGISTER_FUNCTION(Domain)
{
    factory.registerFunction<FunctionDomain>(
        {
        R"(
Extracts the hostname from a URL.

The URL can be specified with or without a scheme.
If the argument can't be parsed as URL, the function returns an empty string.
        )",
        Documentation::Examples{{"domain", "SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')"}},
        Documentation::Categories{"URL"}
        });

    factory.registerFunction<FunctionDomainRFC>(
        {
        R"(Similar to `domain` but follows stricter rules to be compatible with RFC 3986 and less performant.)",
        Documentation::Examples{},
        Documentation::Categories{"URL"}
        });

    factory.registerFunction<FunctionDomainKQL>(
        {
        R"(Similar to `domainRFC` but allows for hostnames without a . in some cases. )",
        Documentation::Examples{},
        Documentation::Categories{"URL"}
        });
}

}
