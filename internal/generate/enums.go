package generate

import (
	"fmt"
	"strings"
	"unicode"
)

// ToPascalCase converts a snake_case or kebab-case string to PascalCase.
func ToPascalCase(s string) string {
	var result strings.Builder

	splitFn := func(r rune) bool {
		return !(unicode.IsLetter(r) || unicode.IsDigit(r))
	}
	parts := strings.FieldsFunc(s, splitFn)

	for _, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(part)
		result.WriteRune(unicode.ToUpper(runes[0]))
		for _, r := range runes[1:] {
			result.WriteRune(unicode.ToLower(r))
		}
	}

	return result.String()
}

// ToKebabCase converts a snake_case string to kebab-case.
func ToKebabCase(s string) string {
	return strings.ReplaceAll(s, "_", "-")
}

// GenerateTypeScriptEnum generates a TypeScript enum string from a type name and its values.
func GenerateTypeScriptEnum(typeName string, values []string) string {
	pascalName := ToPascalCase(typeName)

	var b strings.Builder
	fmt.Fprintf(&b, "export enum %s {\n", pascalName)
	seen := map[string]bool{}
	for _, v := range values {
		fmt.Fprintf(&b, "  %s = %q,\n", resolveMemberName(ToPascalCase(v), seen), v)
	}
	b.WriteString("}\n")

	return b.String()
}

func resolveMemberName(pascal string, seen map[string]bool) string {
	name := pascal
	// leading digits aren't valid identifiers, and a numeric name is rejected
	// even when quoted, so prefix it
	if name != "" && name[0] >= '0' && name[0] <= '9' {
		name = "_" + name
	}
	base := name
	// dedup
	for i := 2; seen[name]; i++ {
		name = fmt.Sprintf("%s_%d", base, i)
	}
	seen[name] = true
	return name
}
