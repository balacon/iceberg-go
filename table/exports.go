// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"sync/atomic"

	"io"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
)

func (b *MetadataBuilder) EnextSequenceNumber() int64 {
	return b.nextSequenceNumber()
}

func (b *MetadataBuilder) EnewSnapshotID() int64 {
	return b.newSnapshotID()
}

func (b *MetadataBuilder) EcurrentSnapshot() *Snapshot {
	return b.currentSnapshot()
}

func (t *Transaction) Etbl() *Table {
	return t.tbl
}

func (t *Transaction) Emeta() *MetadataBuilder {
	return t.meta
}

func (t *Transaction) Eapply(updates []Update, reqs []Requirement) error {
	return t.apply(updates, reqs)
}

type SnapshotProducer = snapshotProducer
type ProducerImpl = producerImpl

func (s *SnapshotProducer) EsetProducerImpl(impl ProducerImpl) {
	s.producerImpl = impl
}

func (s *SnapshotProducer) EcommitUuid() uuid.UUID {
	return s.commitUuid
}

func (s *SnapshotProducer) Eio() iceio.WriteFileIO {
	return s.io
}

func (s *SnapshotProducer) Etxn() *Transaction {
	return s.txn
}

func (s *SnapshotProducer) Eop() Operation {
	return s.op
}

func (s *SnapshotProducer) EsnapshotID() int64 {
	return s.snapshotID
}

func (s *SnapshotProducer) EparentSnapshotID() int64 {
	return s.parentSnapshotID
}

func (s *SnapshotProducer) EaddedFiles() []iceberg.DataFile {
	return s.addedFiles
}

func (s *SnapshotProducer) EdeletedFiles() map[string]iceberg.DataFile {
	return s.deletedFiles
}

func (s *SnapshotProducer) EmanifestCount() *atomic.Int32 {
	return &s.manifestCount
}

func (s *SnapshotProducer) EsnapshotProps() iceberg.Properties {
	return s.snapshotProps
}

func EnewManifestFileName(num int, commit uuid.UUID) string {
	return newManifestFileName(num, commit)
}

func EnewManifestListFileName(snapshotID int64, attempt int, commit uuid.UUID) string {
	return newManifestListFileName(snapshotID, attempt, commit)
}

func EcreateSnapshotProducer(op Operation, txn *Transaction, fs iceio.WriteFileIO, commitUUID *uuid.UUID, snapshotProps iceberg.Properties) *SnapshotProducer {
	return createSnapshotProducer(op, txn, fs, commitUUID, snapshotProps)
}

func (sp *SnapshotProducer) Espec(id int) iceberg.PartitionSpec {
	return sp.spec(id)
}

func (sp *SnapshotProducer) EappendDataFile(df iceberg.DataFile) *SnapshotProducer {
	return sp.appendDataFile(df)
}

func (sp *SnapshotProducer) EdeleteDataFile(df iceberg.DataFile) *SnapshotProducer {
	return sp.deleteDataFile(df)
}

type CountingWriter = internal.CountingWriter

func (sp *SnapshotProducer) EnewManifestWriter(spec iceberg.PartitionSpec) (*iceberg.ManifestWriter, string, *CountingWriter, io.Closer, error) {
	return sp.newManifestWriter(spec)
}

func (sp *SnapshotProducer) EnewManifestOutput() (io.WriteCloser, string, error) {
	return sp.newManifestOutput()
}

func (sp *SnapshotProducer) Emanifests() ([]iceberg.ManifestFile, error) {
	return sp.manifests()
}

func (sp *snapshotProducer) Esummary(props iceberg.Properties) (Summary, error) {
	return sp.summary(props)
}

func (sp *snapshotProducer) Ecommit() ([]Update, []Requirement, error) {
	return sp.commit()
}

func (s *SnapshotSummaryCollector) EsetPartitionSummaryLimit(limit int) {
	s.setPartitionSummaryLimit(limit)
}
func (s *SnapshotSummaryCollector) EaddFile(df iceberg.DataFile, sc *iceberg.Schema, spec iceberg.PartitionSpec) error {
	return s.addFile(df, sc, spec)
}
func (s *SnapshotSummaryCollector) EremoveFile(df iceberg.DataFile, sc *iceberg.Schema, spec iceberg.PartitionSpec) error {
	return s.removeFile(df, sc, spec)
}
func (s *SnapshotSummaryCollector) EpartitionSummary(metrics *updateMetrics) string {
	return s.partitionSummary(metrics)
}
func (s *SnapshotSummaryCollector) Ebuild() iceberg.Properties {
	return s.build()
}
func EupdateSnapshotSummaries(sum Summary, previous iceberg.Properties) (Summary, error) {
	return updateSnapshotSummaries(sum, previous)
}
