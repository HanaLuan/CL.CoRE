package splithttp

type DialRuntime struct {
	HasReality bool
}

func resolveDialMode(config *SplitHTTPConfig, runtime DialRuntime) string {
	if config.Mode != "" && config.Mode != "auto" {
		return config.Mode
	}
	if runtime.HasReality {
		if config.DownloadConfig != nil {
			return "stream-up"
		}
		return "stream-one"
	}
	return "packet-up"
}
