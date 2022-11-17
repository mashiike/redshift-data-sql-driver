package redshiftdatasqldriver

func nullif(str string) *string {
	if str == "" {
		return nil
	}
	return &str
}

func coalesce(strs ...*string) string {
	for _, str := range strs {
		if str != nil {
			return *str
		}
	}
	return ""
}
