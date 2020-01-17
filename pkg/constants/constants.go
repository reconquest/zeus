// TODO: rename package
package constants

const (
	// TODO: move system zfs property names into zfs package
	GUID               = "guid"
	Keystatus          = "keystatus"
	KeystatusAvailable = "available"
	EncryptionRoot     = "encryptionroot"
	Referenced         = "referenced"
	Used               = "used"
)

const (
	Managed                         = "zeus::managed"
	Backup                          = "zeus:backup"
	BackupInterval                  = "zeus:backup:interval"
	Housekeeping                    = "zeus:housekeeping"
	HousekeepingByCountKeepOnTarget = "zeus:housekeeping:by-count:keep-on-target"
	HousekeepingByCountKeepOnSource = "zeus:housekeeping:by-count:keep-on-source"
)
