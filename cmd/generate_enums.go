package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/flags"
	"github.com/pjtatlow/scurry/internal/generate"
	"github.com/pjtatlow/scurry/internal/schema"
)

var outputDir string

var supportedLanguages = map[string]bool{
	"ts": true,
}

var generateEnumsCmd = &cobra.Command{
	Use:   "enums <language>",
	Short: "Generate enums from SQL enum types",
	Long: `Generate enum files from CockroachDB enum types defined in your schema SQL files.

Each enum type produces a separate file in the specified language, named in kebab-case.

Supported languages: ts

Example:
  scurry generate enums ts --definitions ./definitions --output ./src/enums`,
	Args: cobra.ExactArgs(1),
	RunE: generateEnums,
}

func init() {
	generateCmd.AddCommand(generateEnumsCmd)

	flags.AddDefinitionDir(generateEnumsCmd)
	generateEnumsCmd.Flags().StringVar(&outputDir, "output", "", "Output directory for generated TypeScript files")
	generateEnumsCmd.MarkFlagRequired("output")
}

func generateEnums(cmd *cobra.Command, args []string) error {
	lang := args[0]
	if !supportedLanguages[lang] {
		return fmt.Errorf("unsupported language %q (supported: ts)", lang)
	}

	if flags.DefinitionDir == "" {
		return fmt.Errorf("definition directory is required (use --definitions)")
	}

	fs := afero.NewOsFs()
	count, err := doGenerateEnums(fs, flags.DefinitionDir, outputDir, lang)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	fmt.Printf("Generated %d enum file(s) in %s\n", count, outputDir)
	return nil
}

func doGenerateEnums(fs afero.Fs, definitionDir, outDir, lang string) (int, error) {
	// Walk definition dir and parse SQL files
	var allStatements []tree.Statement
	err := afero.Walk(fs, definitionDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(path), ".sql") {
			return nil
		}

		content, err := afero.ReadFile(fs, path)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", path, err)
		}

		statements, err := schema.ParseSQL(string(content))
		if err != nil {
			return fmt.Errorf("in file %s: %w", path, err)
		}

		allStatements = append(allStatements, statements...)
		return nil
	})
	if err != nil {
		return 0, err
	}

	// Filter for enum types and generate
	count := 0
	for _, stmt := range allStatements {
		createType, ok := stmt.(*tree.CreateType)
		if !ok || createType.Variety != tree.Enum {
			continue
		}

		typeName := createType.TypeName.Object()
		values := make([]string, 0, len(createType.EnumLabels))
		for _, label := range createType.EnumLabels {
			values = append(values, string(label))
		}

		var content string
		var ext string
		switch lang {
		case "ts":
			content = generate.GenerateTypeScriptEnum(typeName, values)
			ext = ".ts"
		}
		fileName := generate.ToKebabCase(typeName) + ext

		if err := fs.MkdirAll(outDir, 0755); err != nil {
			return 0, fmt.Errorf("failed to create output directory: %w", err)
		}

		filePath := filepath.Join(outDir, fileName)
		if err := afero.WriteFile(fs, filePath, []byte(content), 0644); err != nil {
			return 0, fmt.Errorf("failed to write %s: %w", filePath, err)
		}

		count++
	}

	return count, nil
}
