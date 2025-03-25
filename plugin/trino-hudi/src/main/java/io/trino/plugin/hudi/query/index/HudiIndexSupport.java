package io.trino.plugin.hudi.query.index;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface HudiIndexSupport
{
    Map<String, List<FileSlice>> lookupCandidateFilesInMetadataTable(Map<String, List<FileSlice>> inputFileSlices, TupleDomain<? extends ColumnHandle> tupleDomain);

    default boolean supportsQueryType(HoodieTableQueryType queryType, Optional<String> specifiedQueryInstant, HoodieTableMetaClient metaClient)
    {
        return true;
    }
}
