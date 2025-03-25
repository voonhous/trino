package io.trino.plugin.hudi.query.index;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.metadata.HoodieTableMetadata;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Factory Pattern to facilitate index initialization
 */
public class IndexSupportFactory
{
    // Use LinkedListHashMap to preserve insertion order of key-val
    private static final Map<IndexSupportType, Function<HoodieTableMetaClient, Boolean>> availabilityCheckers = new LinkedHashMap<>();
    private static final Map<IndexSupportType, BiFunction<HoodieTableMetaClient, HoodieTableMetadata, HudiIndexSupport>> indexConstructors = new LinkedHashMap<>();

    static {
        // Register availability checkers
        availabilityCheckers.put(IndexSupportType.RECORD_LEVEL, HudiRecordLevelIndexSupport::isIndexSupportAvailable);
        availabilityCheckers.put(IndexSupportType.COLUMN_STATS, HudiColumnStatsIndexSupport::isIndexSupportAvailable);

        // Register index constructors
        indexConstructors.put(IndexSupportType.RECORD_LEVEL, HudiRecordLevelIndexSupport::new);
        indexConstructors.put(IndexSupportType.COLUMN_STATS, HudiColumnStatsIndexSupport::new);
    }

    public static boolean isIndexSupportAvailable(IndexSupportType indexType, HoodieTableMetaClient metaClient) {
        return availabilityCheckers.getOrDefault(indexType, _ -> false).apply(metaClient);
    }

    public static List<HudiIndexSupport> initIndexSupport(HoodieTableMetaClient metaClient, HoodieTableMetadata metadataTable) {
        return availabilityCheckers.keySet().stream()
                .filter( indexSupportType -> isIndexSupportAvailable(indexSupportType, metaClient))
                .map( indexType -> indexConstructors.get(indexType).apply(metaClient, metadataTable))
                .toList();
    }
}
