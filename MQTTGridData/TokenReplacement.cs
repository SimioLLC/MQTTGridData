using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MQTTGridData
{
    // See https://stackoverflow.com/questions/1869567/c-sharp-regex-how-can-i-replace-tokens-with-strings-generated-at-run-time
    static class TokenReplacement
    {
        const string TokenMatchRegexString = @"\${([^}]+)}";
        static readonly Regex TokenReplacementRegex = new Regex(TokenMatchRegexString, RegexOptions.Compiled);

        /// <summary>
        /// Replaces tokens in the given string in the form ${token}
        /// </summary>
        /// <param name="originalString">The original string with tokens to replace</param>
        /// <param name="tokenReplacements">The name-value pairs for tokens</param>
        /// <returns>A new string with the tokens replaced with the given values</returns>
        public static string ResolveString(string originalString, IDictionary<string, string> tokenReplacements, IDictionary<string, string> columnValues)
        {
            if (originalString == null)
                return null;

            if (originalString == String.Empty)
                return String.Empty;

            return TokenReplacementRegex.Replace(originalString, match =>
            {
                var token = match.Groups[1].Value;
                if (tokenReplacements != null && tokenReplacements.TryGetValue(token, out var tokenValue))
                {
                    // Token values can themselves reference column values. We "cascade" the replacements from column values -> table token replacements
                    return ResolveString(tokenValue, tokenReplacements, columnValues);
                }

                if (columnValues != null && token.StartsWith("col:", StringComparison.OrdinalIgnoreCase))
                {
                    token = token.Substring(4);
                    if (columnValues.TryGetValue(token, out var columnValue))
                    {
                        return columnValue;
                    }
                }
                if (token == "jsonobject") return "${jsonobject}";
                else if (token == "jsonarray") return "${jsonarray}";
                else return String.Empty;

            });
        }
    }
}
