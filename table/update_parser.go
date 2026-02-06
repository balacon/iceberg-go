// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"encoding/json"
)

func ParseUpdateJSON(b []byte) (Update, error) {
	var base baseUpdate
	if err := json.Unmarshal(b, &base); err != nil {
		return nil, err
	}

	switch base.Action() {
	case UpdateAddSpec:
		var up addPartitionSpecUpdate
		if err := json.Unmarshal(b, &up); err != nil {
			return nil, err
		}
		return NewAddPartitionSpecUpdate(up.Spec, false), nil

	case UpdateAddSchema:
		var up addSchemaUpdate
		if err := json.Unmarshal(b, &up); err != nil {
			return nil, err
		}

		return NewAddSchemaUpdate(up.Schema), nil

	case UpdateAddSnapshot:
		var up addSnapshotUpdate
		if err := json.Unmarshal(b, &up); err != nil {
			return nil, err
		}

		return NewAddSnapshotUpdate(up.Snapshot), nil

	case UpdateAddSortOrder:
		var up addSortOrderUpdate
		if err := json.Unmarshal(b, &up); err != nil {
			return nil, err
		}

		return NewAddSortOrderUpdate(up.SortOrder), nil

	case UpdateAssignUUID:
		var up assignUUIDUpdate
		if err := json.Unmarshal(b, &up); err != nil {
			return nil, err
		}

		return NewAssignUUIDUpdate(up.UUID), nil

	case UpdateRemoveProperties:
		var up removePropertiesUpdate
		if err := json.Unmarshal(b, &up); err != nil {
			return nil, err
		}

		return NewRemovePropertiesUpdate(up.Removals), nil

	case UpdateRemoveSnapshots:
		var up removeSnapshotsUpdate
		if err := json.Unmarshal(b, &up); err != nil {
			return nil, err
		}

		return NewRemoveSnapshotsUpdate(up.SnapshotIDs), nil

	case UpdateRemoveSnapshotRef:
		var up removeSnapshotRefUpdate
		if err := json.Unmarshal(b, &up); err != nil {
			return nil, err
		}

		return NewRemoveSnapshotRefUpdate(up.RefName), nil

	case UpdateSetCurrentSchema:
		var up setCurrentSchemaUpdate
		if err := json.Unmarshal(b, &up); err != nil {
			return nil, err
		}

		return NewSetCurrentSchemaUpdate(up.SchemaID), nil

	case UpdateSetDefaultSortOrder:
		var up setDefaultSortOrderUpdate
		if err := json.Unmarshal(b, &up); err != nil {
			return nil, err
		}

		return NewSetDefaultSortOrderUpdate(up.SortOrderID), nil

	case UpdateSetDefaultSpec:
		var up setDefaultSpecUpdate
		if err := json.Unmarshal(b, &up); err != nil {
			return nil, err
		}

		return NewSetDefaultSpecUpdate(up.SpecID), nil

	case UpdateSetLocation:
		var up setLocationUpdate
		if err := json.Unmarshal(b, &up); err != nil {
			return nil, err
		}

		return NewSetLocationUpdate(up.Location), nil

	case UpdateSetProperties:
		var up setPropertiesUpdate
		if err := json.Unmarshal(b, &up); err != nil {
			return nil, err
		}

		return NewSetPropertiesUpdate(up.Updates), nil

	case UpdateSetSnapshotRef:
		var up setSnapshotRefUpdate
		if err := json.Unmarshal(b, &up); err != nil {
			return nil, err
		}

		return NewSetSnapshotRefUpdate(
			up.RefName,
			up.SnapshotID,
			up.RefType,
			up.MaxRefAgeMs,
			up.MaxSnapshotAgeMs,
			up.MinSnapshotsToKeep,
		), nil

	case UpdateUpgradeFormatVersion:
		var up upgradeFormatVersionUpdate
		if err := json.Unmarshal(b, &up); err != nil {
			return nil, err
		}

		return NewUpgradeFormatVersionUpdate(up.FormatVersion), nil
	}

	return nil, ErrInvalidRequirement
}
