package io.trino.plugin.hudi.query;

import io.airlift.log.Logger;
import io.trino.plugin.hudi.partition.HiveHudiPartitionInfo;
import io.trino.plugin.hudi.query.index.HudiIndexSupport;
import io.trino.plugin.hudi.query.index.IndexSupportFactory;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieTableMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class HudiFileSkippingManager
{
    private static final Logger log = Logger.get(HudiFileSkippingManager.class);

    private final HoodieTableQueryType queryType;
    private final Optional<String> specifiedQueryInstant;
    private final HoodieTableMetaClient metaClient;
    private final HoodieTableMetadata metadataTable;

    private final Map<String, List<FileSlice>> allInputFileSlices;

    private final List<HudiIndexSupport> supportedIndices;

    public HudiFileSkippingManager(
            List<HiveHudiPartitionInfo> partitions,
            String spillableDir,
            HoodieEngineContext engineContext,
            HoodieTableMetaClient metaClient,
            HoodieTableQueryType queryType,
            Optional<String> specifiedQueryInstant)
    {
        requireNonNull(partitions, "partitions is null");
        requireNonNull(spillableDir, "spillableDir is null");
        requireNonNull(engineContext, "engineContext is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.specifiedQueryInstant = requireNonNull(specifiedQueryInstant, "specifiedQueryInstant is null");
        this.metaClient = requireNonNull(metaClient, "metaClient is null");

        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
        this.metadataTable = HoodieTableMetadata.create(
                engineContext, metaClient.getStorage(), metadataConfig, metaClient.getBasePath().toString(), true);
        this.supportedIndices = IndexSupportFactory.initIndexSupport(metaClient, metadataTable);
        this.allInputFileSlices = prepareAllInputFileSlices(partitions, engineContext, spillableDir);
    }

    private Map<String, List<FileSlice>> prepareAllInputFileSlices(
            List<HiveHudiPartitionInfo> partitions,
            HoodieEngineContext engineContext,
            String spillableDir)
    {
        long startTime = System.currentTimeMillis();
        HoodieTimeline activeTimeline = metaClient.reloadActiveTimeline();
        Optional<HoodieInstant> latestInstant = activeTimeline.lastInstant().toJavaOptional();
        // build system view.
        SyncableFileSystemView fileSystemView = FileSystemViewManager
                .createViewManager(engineContext,
                        FileSystemViewStorageConfig.newBuilder().withBaseStoreDir(spillableDir).build(),
                        HoodieCommonConfig.newBuilder().build(),
                        e -> metadataTable)
                .getFileSystemView(metaClient);
        Optional<String> queryInstant = specifiedQueryInstant.isPresent() ?
                specifiedQueryInstant : latestInstant.map(HoodieInstant::requestedTime);

        Map<String, List<FileSlice>> allInputFileSlices = engineContext
                .mapToPair(
                        partitions,
                        partitionPath -> Pair.of(
                                partitionPath.getHivePartitionName(),
                                getLatestFileSlices(partitionPath.getRelativePartitionPath(), fileSystemView, queryInstant)),
                        partitions.size());

        long duration = System.currentTimeMillis() - startTime;
        log.debug("prepare query files for table %s, spent: %d ms", metaClient.getTableConfig().getTableName(), duration);
        return allInputFileSlices;
    }

    private List<FileSlice> getLatestFileSlices(
            String partitionPath,
            SyncableFileSystemView fileSystemView,
            Optional<String> queryInstant)
    {
        return queryInstant
                .map(instant ->
                        fileSystemView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, queryInstant.get()))
                .orElse(fileSystemView.getLatestFileSlices(partitionPath))
                .collect(Collectors.toList());
    }

    public Map<String, List<FileSlice>> listQueryFiles(TupleDomain<? extends ColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll()) {
            return allInputFileSlices;
        }
        // Do file skipping by MetadataTable
        Map<String, List<FileSlice>> candidateFileSlices = allInputFileSlices;
        int totalFileSlices = allInputFileSlices.values().stream().mapToInt(List::size).sum();
        try {
            for (HudiIndexSupport index : supportedIndices) {
                candidateFileSlices = index.lookupCandidateFilesInMetadataTable(allInputFileSlices, tupleDomain);
                if (candidateFileSlices.values().stream().mapToInt(List::size).sum() < totalFileSlices) {
                    // Terminate if there are SOME files pruned
                    break;
                }
            }
        }
        catch (Exception e) {
            // Should not throw exception, just log this Exception.
            log.warn(e, "failed to do data skipping for table: %s, fallback to all files scan", metaClient.getBasePath());
            candidateFileSlices = allInputFileSlices;
        }
        if (log.isDebugEnabled()) {
            int candidateFileSize = candidateFileSlices.values().stream().mapToInt(List::size).sum();
            int totalFiles = allInputFileSlices.values().stream().mapToInt(List::size).sum();
            double skippingPercent = totalFiles == 0 ? 0.0d : (totalFiles - candidateFileSize) / (totalFiles + 0.0d);
            log.debug("Total files: %s; candidate files after data skipping: %s; skipping percent %s",
                    totalFiles,
                    candidateFileSize,
                    skippingPercent);
        }
        return candidateFileSlices;
    }
}
