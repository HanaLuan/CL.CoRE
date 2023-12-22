package statistic

type RequestNotify func(c Tracker)

var DefaultRequestNotify RequestNotify

func (m *Manager) Current(onlyProxy ...bool) (up, down int64) {
	if len(onlyProxy) > 0 && onlyProxy[0] {
		return m.proxyUploadBlip.Load(), m.proxyDownloadBlip.Load()
	}
	return m.uploadBlip.Load(), m.downloadBlip.Load()
}
