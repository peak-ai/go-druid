package dsql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanParseURLs(t *testing.T) {
	type testCase struct {
		input    string
		expected string
	}

	cases := []testCase{
		{
			input:    "test.com/route",
			expected: "http://test.com/route?pingEndpoint=/status/health&queryEndpoint=/druid/v2/sql",
		},
		{
			input:    "druid://test.com/route",
			expected: "http://test.com/route?pingEndpoint=/status/health&queryEndpoint=/druid/v2/sql",
		},
		{
			input:    "druid://test.com?sslenable=true",
			expected: "https://test.com?pingEndpoint=/status/health&queryEndpoint=/druid/v2/sql&sslenable=false",
		},
		{
			input:    "//127.0.0.1:8080",
			expected: "http://127.0.0.1:8080?pingEndpoint=/status/health&queryEndpoint=/druid/v2/sql",
		},
		{
			input:    "http://127.0.0.1:8080",
			expected: "http://127.0.0.1:8080?pingEndpoint=/status/health&queryEndpoint=/druid/v2/sql",
		},
	}

	for _, tc := range cases {
		actual := ParseDSN(tc.input).FormatDSN()
		require.Equal(t, tc.expected, actual)
	}
}
