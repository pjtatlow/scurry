package generate

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToPascalCase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "snake_case",
			input:    "user_status",
			expected: "UserStatus",
		},
		{
			name:     "UPPER_CASE",
			input:    "USER_STATUS",
			expected: "UserStatus",
		},
		{
			name:     "single word",
			input:    "active",
			expected: "Active",
		},
		{
			name:     "already PascalCase",
			input:    "UserStatus",
			expected: "Userstatus",
		},
		{
			name:     "kebab-case",
			input:    "user-status",
			expected: "UserStatus",
		},
		{
			name:     "mixed delimiters",
			input:    "some_kebab-case_mix",
			expected: "SomeKebabCaseMix",
		},
		{
			name:     "single uppercase word",
			input:    "ACTIVE",
			expected: "Active",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ToPascalCase(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToKebabCase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "snake_case",
			input:    "user_status",
			expected: "user-status",
		},
		{
			name:     "single word",
			input:    "active",
			expected: "active",
		},
		{
			name:     "already kebab-case",
			input:    "user-status",
			expected: "user-status",
		},
		{
			name:     "multiple underscores",
			input:    "some_long_type_name",
			expected: "some-long-type-name",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ToKebabCase(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGenerateTypeScriptEnum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		typeName string
		values   []string
		expected string
	}{
		{
			name:     "basic enum",
			typeName: "user_status",
			values:   []string{"active", "inactive", "pending_review"},
			expected: `export enum UserStatus {
  Active = "active",
  Inactive = "inactive",
  PendingReview = "pending_review",
}
`,
		},
		{
			name:     "single value",
			typeName: "color",
			values:   []string{"red"},
			expected: `export enum Color {
  Red = "red",
}
`,
		},
		{
			name:     "UPPER_CASE values",
			typeName: "priority",
			values:   []string{"LOW", "MEDIUM", "HIGH"},
			expected: `export enum Priority {
  Low = "LOW",
  Medium = "MEDIUM",
  High = "HIGH",
}
`,
		},
		{
			name:     "kebab-case values",
			typeName: "order_status",
			values:   []string{"in-progress", "on-hold", "completed"},
			expected: `export enum OrderStatus {
  InProgress = "in-progress",
  OnHold = "on-hold",
  Completed = "completed",
}
`,
		},
		{
			name:     "values with special characters",
			typeName: "notification_channel",
			values:   []string{"email", "sms", "in-app_push"},
			expected: `export enum NotificationChannel {
  Email = "email",
  Sms = "sms",
  InAppPush = "in-app_push",
}
`,
		},
		{
			name:     "empty values",
			typeName: "empty_enum",
			values:   []string{},
			expected: `export enum EmptyEnum {
}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := GenerateTypeScriptEnum(tt.typeName, tt.values)
			assert.Equal(t, tt.expected, result)
		})
	}
}
