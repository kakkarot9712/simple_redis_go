//go:build ignore
// +build ignore

//go:generate go run ./main.go

package main

import (
	"bytes"
	"fmt"
	"go/format"
	"log"
	"os"
	"os/exec"
	"slices"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

type PlacementType string

const (
	INDEX PlacementType = "index"
	AFTER PlacementType = "after"
)

type SpecConfig struct {
	Name       string `yaml:"name"`
	Type       string `yaml:"type"`
	Optional   bool   `yaml:"optional"`
	Rest       bool   `yaml:"rest"`
	AfterField string `yaml:"afterField"`
}

type ArgsConfig struct {
	Min  int          `yaml:"min"`
	Max  int          `yaml:"max"`
	Spec []SpecConfig `yaml:"spec"`
}

type CmdConfig struct {
	Name                     string     `yaml:"name"`
	Timestamp                bool       `yaml:"timestamp"`
	AutoGenerateScalerParser bool       `yaml:"autoGenerateScalerParser"`
	Args                     ArgsConfig `yaml:"args"`
}

type Config struct {
	Version  uint8       `yaml:"version"`
	Commands []CmdConfig `yaml:"commands"`
	Out      *string     `yaml:"out"`
}

func main() {
	configPath := "../commands.yml"
	outPath := "../commands.go"
	tplPath := "./commands.tpl"

	var buf bytes.Buffer
	var data Config

	configBuff, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("config file reading failed: %v", err)
	}
	err = yaml.Unmarshal(configBuff, &data)
	if err != nil {
		log.Fatalf("config file parsing failed: %v", err)
	}
	if data.Out != nil {
		outPath = *data.Out
	}
	if err := executeTemplate(tplPath, data, &buf); err != nil {
		log.Fatalf("template execution failed: %v", err)
	}
	formatted, err := runGoimports(buf.Bytes())
	if err != nil {
		log.Printf("goimports unavailable, falling back to go/format: %v", err)

		// --- Fallback: stdlib go/format ---
		formatted, err = format.Source(buf.Bytes())
		if err != nil {
			log.Printf("go/format failed — raw output:\n%s", buf.String())
			log.Fatalf("format.Source: %v", err)
		}
	}
	if err := os.WriteFile(outPath, formatted, 0o644); err != nil {
		log.Fatalf("writing output file: %v", err)
	}

	fmt.Printf("generated: %s\n", outPath)
}

func executeTemplate(tplPath string, data Config, buf *bytes.Buffer) error {
	// Read the template file from disk.
	// Keeping templates on disk (not embedded) means you can edit them
	// without recompiling the generator.
	tplSrc, err := os.ReadFile(tplPath)
	if err != nil {
		return fmt.Errorf("reading template %q: %w", tplPath, err)
	}

	// Parse the template. Name it after the file for clearer error messages.
	tmpl, err := template.New(tplPath).Funcs(templateFuncs()).Parse(string(tplSrc))
	if err != nil {
		return fmt.Errorf("parsing template %q: %w", tplPath, err)
	}

	// Execute — any template error will surface here with line numbers.
	if err := tmpl.Execute(buf, data); err != nil {
		return fmt.Errorf("executing template %q: %w", tplPath, err)
	}

	return nil
}

func goType(spec SpecConfig) string {
	goType := ""
	switch spec.Type {
	case "string_bulk":
		goType = "string"
	case "int":
		goType = "int64"
	case "float":
		goType = "float64"
	case "uint":
		goType = "uint64"
	case "raw":
		goType = "Token"
	default:
		goType = spec.Type
	}
	if isOptional(spec) {
		return "*" + goType
	}
	return goType
}

func isOptional(spec SpecConfig) bool {
	return spec.Optional
}

func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"toLower": func(data string) string {
			return strings.ToLower(data)
		},
		"toUpperFirst": func(data string) string {
			if data == "" {
				return data
			}
			return strings.ToUpper(data[:1]) + data[1:]
		},
		"goType": goType,
		"inList": func(val string, items ...string) bool {
			return slices.Contains(items, val)
		},
		"isPointer": func(typ string) bool {
			return strings.HasPrefix(typ, "*")
		},
		"filterArgs": func(args ArgsConfig, typ PlacementType) []SpecConfig {
			flags := make([]SpecConfig, 0)
			for _, arg := range args.Spec {
				isIndex := arg.AfterField == ""
				if typ == INDEX && isIndex || typ == AFTER && !isIndex {
					flags = append(flags, arg)
				}
			}
			return flags
		},
	}
}

// go install golang.org/x/tools/cmd/goimports@latest
func runGoimports(src []byte) ([]byte, error) {
	goimports, err := exec.LookPath("goimports")
	if err != nil {
		return nil, fmt.Errorf("goimports not found on PATH: %w", err)
	}

	cmd := exec.Command(goimports)

	cmd.Stdin = bytes.NewReader(src)

	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("goimports: %w\nstderr: %s", err, stderr.String())
	}

	return out.Bytes(), nil
}
