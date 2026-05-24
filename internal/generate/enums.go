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
		return r == '_' || r == '-'
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
	for _, v := range values {
		fmt.Fprintf(&b, "  %s = %q,\n", ToPascalCase(v), v)
	}
	b.WriteString("}\n")

	return b.String()
}

func GenerateTypeScriptZodEnum(typeName string, values []string) string {
	pascalName := ToPascalCase(typeName)
	valuesName := fmt.Sprintf("%sValues", pascalName)

	var b strings.Builder
	fmt.Fprintf(&b, "import * as z from \"zod/v4\";\n\n")
	fmt.Fprintf(&b, "export const %s = [\n", valuesName)
	for _, v := range values {
		fmt.Fprintf(&b, "  %q,\n", v)
	}
	b.WriteString("] as const;\n")
	fmt.Fprintf(&b, "export const %sSchema = z.enum(%s);\n", pascalName, valuesName)
	fmt.Fprintf(&b, "export const %s = %sSchema.enum;\n", pascalName, pascalName)
	fmt.Fprintf(&b, "export type %s = z.infer<typeof %sSchema>;\n", pascalName, pascalName)

	return b.String()
}
