package redshiftdatasqldriver

func nullif(str string) *string {
	if str == "" {
		return nil
	}
	return &str
}

func coalesce(str *string) string {
	if str == nil {
		return ""
	}
	return *str
}
