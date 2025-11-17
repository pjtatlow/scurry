package ui

import (
	"strings"

	"github.com/alecthomas/chroma/v2/formatters"
	"github.com/alecthomas/chroma/v2/lexers"
	"github.com/alecthomas/chroma/v2/styles"
	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/lipgloss"
)

var (
	// Colors
	Text   = lipgloss.Color("#cdd6f4")
	Red    = lipgloss.Color("#f38ba8")
	Green  = lipgloss.Color("#a6e3a1")
	Yellow = lipgloss.Color("#f9e2af")
	Blue   = lipgloss.Color("#89b4fa")
	Gray   = lipgloss.Color("#6c7086")
	Pink   = lipgloss.Color("#f5c2e7")

	// Styles for different message types
	ErrorStyle = lipgloss.NewStyle().
			Foreground(Red).
			Bold(true)

	WarningStyle = lipgloss.NewStyle().
			Foreground(Yellow).
			Bold(true)

	SuccessStyle = lipgloss.NewStyle().
			Foreground(Green).
			Bold(true)

	InfoStyle = lipgloss.NewStyle().
			Foreground(Blue)

	SubtleStyle = lipgloss.NewStyle().
			Foreground(Gray)

	DestructiveStyle = lipgloss.NewStyle().
				Foreground(Red).
				Bold(true)

	HeaderStyle = lipgloss.NewStyle().
			Bold(true).
			Underline(true)

	// Specific component styles
	WarningBannerStyle = lipgloss.NewStyle().
				Foreground(Red).
				Bold(true).
				Border(lipgloss.DoubleBorder()).
				BorderForeground(Red).
				Padding(1, 2).
				Margin(1, 0)

	SuccessBannerStyle = lipgloss.NewStyle().
				Foreground(Green).
				Bold(true).
				Border(lipgloss.RoundedBorder()).
				BorderForeground(Green).
				Padding(0, 2)
)

// Helper functions for common formatting

// Error returns red, bold error text
func Error(text string) string {
	return ErrorStyle.Render(text)
}

// Warning returns yellow, bold warning text
func Warning(text string) string {
	return WarningStyle.Render(text)
}

// Success returns green, bold success text
func Success(text string) string {
	return SuccessStyle.Render(text)
}

// Info returns blue info text
func Info(text string) string {
	return InfoStyle.Render(text)
}

// Subtle returns gray subtle text
func Subtle(text string) string {
	return SubtleStyle.Render(text)
}

// Destructive returns red, bold text for destructive operations
func Destructive(text string) string {
	return DestructiveStyle.Render(text)
}

// Header returns bold, underlined header text
func Header(text string) string {
	return HeaderStyle.Render(text)
}

// WarningBanner creates a red bordered warning box
func WarningBanner(text string) string {
	return WarningBannerStyle.Render(text)
}

// SuccessBanner creates a green bordered success box
func SuccessBanner(text string) string {
	return SuccessBannerStyle.Render(text)
}

func SqlCode(text string) string {
	lexer := lexers.Get("postgresql")
	iterator, err := lexer.Tokenise(nil, text)
	if err != nil {
		panic(err)
	}
	formatter := formatters.Get("terminal16m")
	style := styles.Get("catppuccin-mocha")

	s := strings.Builder{}
	if err := formatter.Format(&s, style, iterator); err != nil {
		panic(err)
	}

	return s.String()
}

// HuhTheme returns the custom theme for all huh forms
// Uses white text and green highlighting for consistency
func HuhTheme() *huh.Theme {
	theme := huh.ThemeBase()

	// Title and base text in white
	theme.Focused.Title = lipgloss.NewStyle().Foreground(Text).Bold(true)
	theme.Focused.Base = lipgloss.NewStyle().Foreground(Text)
	theme.Focused.Description = lipgloss.NewStyle().Foreground(Text)
	theme.Focused.TextInput.Cursor = lipgloss.NewStyle().Foreground(Green)
	theme.Focused.TextInput.Placeholder = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	theme.Focused.TextInput.Prompt = lipgloss.NewStyle().Foreground(Green)

	// Selected option in green
	theme.Focused.SelectedOption = lipgloss.NewStyle().Foreground(Green)

	// Focused button - green background with black text
	theme.Focused.FocusedButton = lipgloss.NewStyle().
		Background(Green).
		Foreground(lipgloss.Color("0")).
		Bold(true).
		Padding(0, 1)

	// Blurred/unselected button - white text
	theme.Focused.BlurredButton = lipgloss.NewStyle().Foreground(Text).Padding(0, 1)

	// Unfocused styles
	theme.Blurred.Title = lipgloss.NewStyle().Foreground(Text)
	theme.Blurred.Base = lipgloss.NewStyle().Foreground(Text)
	theme.Blurred.Description = lipgloss.NewStyle().Foreground(Text)

	return theme
}

// ConfirmPrompt displays a yes/no confirmation prompt using huh
// Returns true if user confirms, false otherwise
func ConfirmPrompt(question string) (bool, error) {
	var confirmed bool

	form := huh.NewForm(
		huh.NewGroup(
			huh.NewConfirm().
				Title(question).
				Value(&confirmed),
		),
	).WithTheme(HuhTheme())

	err := form.Run()
	if err != nil {
		return false, err
	}

	return confirmed, nil
}
