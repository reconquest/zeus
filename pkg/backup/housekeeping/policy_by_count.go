package housekeeping

import (
	"fmt"
	"strconv"

	"github.com/reconquest/karma-go"
	"github.com/reconquest/zeus/pkg/backup/operation"
	"github.com/reconquest/zeus/pkg/config"
	"github.com/reconquest/zeus/pkg/constants"
	"github.com/reconquest/zeus/pkg/text"
	"github.com/reconquest/zeus/pkg/zfs"
)

type PolicyByCount struct {
	KeepOnSource int
	KeepOnTarget int
	HoldTag      string
}

func init() {
	Properties = append(Properties, []zfs.PropertyRequest{
		{Name: constants.HousekeepingByCountKeepOnTarget, Inherited: true},
		{Name: constants.HousekeepingByCountKeepOnSource, Inherited: true},
	}...)
}

func NewPolicyByCount(
	config *config.Config,
	properties []zfs.Property,
) (Policy, error) {
	var policy PolicyByCount

	policy.HoldTag = config.HoldTag

	for _, property := range properties {
		switch property.Name {
		case constants.HousekeepingByCountKeepOnSource:
			count, err := parsePolicyByCountKeepProperty(property.Value)
			if err != nil {
				return nil, err
			}

			policy.KeepOnSource = count

		case constants.HousekeepingByCountKeepOnTarget:
			count, err := parsePolicyByCountKeepProperty(property.Value)
			if err != nil {
				return nil, err
			}

			policy.KeepOnTarget = count
		}
	}

	// TODO: validate value
	if policy.KeepOnSource == 0 {
		policy.KeepOnSource = config.Defaults.Housekeeping.ByCount.KeepOnSource
	}

	// TODO: validate value
	if policy.KeepOnTarget == 0 {
		policy.KeepOnTarget = config.Defaults.Housekeeping.ByCount.KeepOnTarget
	}

	return policy, nil
}

func (PolicyByCount) GetName() string {
	return "by-count"
}

func (policy PolicyByCount) Cleanup(operation operation.Backup) error {
	log := log.NewChildWithPrefix("{houseskeeping} <by-count>")

	log.Infof(
		"configuration: will keep %d source %s, %d target %s",
		policy.KeepOnSource,
		text.Pluralize("snapshot", policy.KeepOnSource),
		policy.KeepOnTarget,
		text.Pluralize("snapshot", policy.KeepOnTarget),
	)

	cleanup := func(dataset string, keep int) (int, error) {
		log.Infof(
			"running snapshots cleanup for dataset %q (will keep %d %s)",
			dataset,
			keep,
			text.Pluralize("snapshot", keep),
		)

		snapshots, err := listManagedSnapshots(dataset)
		if err != nil {
			return 0, err
		}

		var destroyed int

		for i, _ := range snapshots {
			_, candidate, err := zfs.SplitSnapshotName(snapshots[i])
			if err != nil {
				return 0, err
			}

			log.Debugf(
				"(%2d of %2d) checking: should snapshot %q be destroyed",
				len(snapshots)-i,
				len(snapshots),
				snapshots[i],
			)

			if i >= len(snapshots)-keep {
				log.Debugf(
					"(%2d of %2d) stop: snapshot index <= %d",
					len(snapshots)-i,
					len(snapshots),
					keep,
				)
				break
			}

			// doubled check that we're not deleting backup snapshot
			if operation.Snapshot.Current == candidate {
				continue
			}

			log.Infof("destroying old snapshot %q", snapshots[i])

			held, err := zfs.HasHold(policy.HoldTag, snapshots[i])
			if err != nil {
				return 0, karma.Format(
					err,
					"unable to check that snapshot %q is held by %q tag",
					snapshots[i],
					policy.HoldTag,
				)
			}

			if held {
				log.Warningf(
					"snapshot %q is still held by %q tag, releasing it",
					snapshots[i],
					policy.HoldTag,
				)

				err := zfs.Release(policy.HoldTag, snapshots[i])
				if err != nil {
					return 0, karma.Format(
						err,
						"unable to release hold tag %q on %q",
						policy.HoldTag,
						snapshots[i],
					)
				}
			}

			err = zfs.DestroyDataset(snapshots[i])
			if err != nil {
				return 0, karma.Format(
					err,
					"unabled to destroy snapshot %q during cleanup",
					snapshots[i],
				)
			}

			destroyed++
		}

		return destroyed, nil
	}

	sourceDestroyed, err := cleanup(operation.Source, policy.KeepOnSource)
	if err != nil {
		return karma.Format(
			err,
			"unable to cleanup snapshots on source dataset",
		)
	}

	targetDestroyed, err := cleanup(
		fmt.Sprintf("%s/%s", operation.Target, operation.Source),
		policy.KeepOnTarget,
	)
	if err != nil {
		return karma.Format(
			err,
			"unable to cleanup snapshots on target dataset",
		)
	}

	log.Infof(
		"housekeeping completed: destroyed %d %s on source and %d %s on target",
		sourceDestroyed, text.Pluralize("snapshot", sourceDestroyed),
		targetDestroyed, text.Pluralize("snapshot", targetDestroyed),
	)

	return nil
}

func listManagedSnapshots(dataset string) ([]string, error) {
	properties, err := zfs.GetDatasetProperties([]zfs.PropertyRequest{
		{Name: constants.Managed, Snapshot: true, Local: true},
	}, dataset)
	if err != nil {
		return nil, err
	}

	snapshots := []string{}

	for _, property := range properties {
		snapshots = append(snapshots, property.Source)
	}

	return snapshots, nil
}

func parsePolicyByCountKeepProperty(value string) (int, error) {
	count, err := strconv.Atoi(value)
	if err != nil {
		return 0, karma.Format(err, "unexpected non-number value")
	}

	if count < 1 {
		return 0, fmt.Errorf(
			"at least one snapshot should be kept for incremental backup to work",
		)
	}

	return count, nil
}
