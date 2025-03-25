package io.trino.plugin.hudi.query.index;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.HudiPredicates;
import io.trino.plugin.hudi.partition.HiveHudiPartitionInfo;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class HudiRecordLevelIndexSupport implements HudiIndexSupport
{
    private static final Logger log = Logger.get(HudiRecordLevelIndexSupport.class);

    public static final String DEFAULT_COLUMN_VALUE_SEPARATOR = ":";
    public static final String DEFAULT_RECORD_KEY_PARTS_SEPARATOR = ",";

    private final HoodieTableMetaClient metaClient;
    private final HoodieTableMetadata metadataTable;

    public HudiRecordLevelIndexSupport(HoodieTableMetaClient metaClient, HoodieTableMetadata metadataTable)
    {
        this.metaClient = requireNonNull(metaClient, "metaClient is null");
        this.metadataTable = requireNonNull(metadataTable, "metadataTable is null");
    }

    @Override
    public Map<String, List<FileSlice>> lookupCandidateFilesInMetadataTable(Map<String, List<FileSlice>> inputFileSlices, TupleDomain<? extends ColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll()) {
            return inputFileSlices;
        }

        TupleDomain<HiveColumnHandle> regularTupleDomain = HudiPredicates.from(tupleDomain).getRegularColumnPredicates();
        TupleDomain<String> regularColumnPredicates = regularTupleDomain.transformKeys(HiveColumnHandle::getName);

        if (metaClient.getTableConfig().getRecordKeyFields().isEmpty()) {
            throw new IllegalArgumentException("Record key fields is empty");
        }
        List<String> recordKeyFields = Arrays.stream(metaClient.getTableConfig().getRecordKeyFields().get()).collect(Collectors.toList());
        TupleDomain<String> filteredDomains = extractPredicatesForColumns(regularColumnPredicates, recordKeyFields);
        List<String> recordKeys = constructRecordKeys(filteredDomains, recordKeyFields.stream().toList());
        Map<String, List<HoodieRecordGlobalLocation>> recordIndex = metadataTable.readRecordIndex(recordKeys);

        if (recordIndex.isEmpty()) {
            return inputFileSlices;
        }

        // Prune files
        List<String> fileIds = recordIndex.values().stream().flatMap(List::stream).map(HoodieRecordGlobalLocation::getFileId).toList();
        Map<String, List<FileSlice>> candidateFileSlices = inputFileSlices
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry
                        .getValue()
                        .stream()
                        .filter(fileSlice -> fileIds.contains(fileSlice.getFileId()))
                        .collect(Collectors.toList())));

        if (log.isInfoEnabled()) {
            int candidateFileSize = candidateFileSlices.values().stream().mapToInt(List::size).sum();
            int totalFiles = inputFileSlices.values().stream().mapToInt(List::size).sum();
            double skippingPercent = totalFiles == 0 ? 0.0d : (totalFiles - candidateFileSize) / (totalFiles * 1.0d);
            log.info("Total files: %s; files after data skipping: %s; skipping percent %s",
                    totalFiles,
                    candidateFileSize,
                    skippingPercent);
        }

        return candidateFileSlices;
    }

    @Override
    public boolean supportsQueryType(HoodieTableQueryType queryType, Optional<String> specifiedQueryInstant, HoodieTableMetaClient metaClient) {
        if (queryType != HoodieTableQueryType.SNAPSHOT) {
            // Disallow RLI for non-snapshot query types
            return false;
        }

        // Now handle the time-travel case for snapshot queries
        if (specifiedQueryInstant.isEmpty()) {
            // No time travel instant specified, so allow if it's a snapshot query
            return true;
        } else {
            // Check if the as.of.instant is greater than or equal to the last completed instant.
            // We can still use RLI for data skipping for the latest snapshot.
            return InstantComparison.compareTimestamps(
                    specifiedQueryInstant.get(),
                    InstantComparison.GREATER_THAN_OR_EQUALS,
                    metaClient.getCommitsTimeline().filterCompletedInstants().lastInstant().get().requestedTime());
        }
    }

    /**
     * Extracts predicates from a TupleDomain that match a given set of columns.
     * Preserves all complex predicate properties including multi-value domains,
     * range-based predicates, and nullability.
     *
     * @param tupleDomain The source TupleDomain containing all predicates
     * @param columnFields The set of columns for which to extract predicates
     * @return A new TupleDomain containing only the predicates for the specified columns
     */
    public static TupleDomain<String> extractPredicatesForColumns(TupleDomain<String> tupleDomain, List<String> columnFields)
    {
        if (tupleDomain.isNone()) {
            return TupleDomain.none();
        }

        if (tupleDomain.isAll()) {
            return TupleDomain.all();
        }

        // Extract the domains matching the specified columns
        Map<String, Domain> allDomains = tupleDomain.getDomains().get();
        Map<String, Domain> filteredDomains = allDomains.entrySet().stream().filter(entry -> columnFields.contains(entry.getKey())) // Ensure key is in the column set
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // If no domains matched, but we had some columns to extract, return ALL
        if (filteredDomains.isEmpty() && !columnFields.isEmpty() && !allDomains.isEmpty()) {
            return TupleDomain.all();
        }

        return TupleDomain.withColumnDomains(filteredDomains);
    }

    /**
     * Constructs a record key from TupleDomain based on whether it's a complex key or not.
     * <p>
     * Construction of record keys will only be handled for domains generated from EQUALITY or IN predicates.
     * <p>
     * An empty list of record keys will be generated if the following conditions are not met:
     * <ol>
     * <li>recordKeysFields is empty</li>
     * <li>recordKeyDomains isAll</li>
     * <li>For the case of complex key, domains are not applied to all recordKeysFields</li>
     * <li>For the case of complex key, domains are applied to all recordKeyFields, but one of the domain is <b>NOT</b>
     * generated from an equality or IN predicate</li>
     * </ol>
     * <p>
     * Note: This function is O(m^n) where m is the average size of value literals and n is the number of record keys.
     * <p>
     * Optimization 1: If MDT enabled functions allows for streams to be passed in, we can implement an iterator to be more memory efficient.
     * <p>
     * Optimization 2: We should also consider limiting the number of recordKeys generated, if it is estimated to be more than a limit, RLI should just be skipped
     * as it may just be faster to read out all data and filer accordingly.
     *
     * @param recordKeyDomains The filtered TupleDomain containing column handles and values
     * @param recordKeyFields List of column names that represent the record keys
     * @return List of string values representing the record key(s)
     */
    public static List<String> constructRecordKeys(TupleDomain<String> recordKeyDomains, List<String> recordKeyFields)
    {
        // If no recordKeys or no recordKeyDomains, return empty list
        if (recordKeyFields == null || recordKeyFields.isEmpty() || recordKeyDomains.isAll()) {
            return Collections.emptyList();
        }

        // All recordKeys must have a domain else, return empty list (applicable to complexKeys)
        // If a one of the recordKey in the set of complexKeys does not have a domain, we are unable to construct
        // a complete complexKey
        if (!recordKeyDomains.getDomains().get().keySet().containsAll(recordKeyFields)) {
            return Collections.emptyList();
        }

        // Extract the domain mappings from the tuple domain
        Map<String, Domain> domains = recordKeyDomains.getDomains().get();

        // Case 1: Not a complex key (single record key)
        if (recordKeyFields.size() == 1) {
            String recordKey = recordKeyFields.getFirst();

            // Extract value for this key
            Domain domain = domains.get(recordKey);
            return extractStringValues(domain);
        }
        // Case 2: Complex/Composite key (multiple record keys)
        else {
            // Create a queue to manage the Cartesian product generation
            Queue<String> results = new LinkedList<>();

            // For each key in the complex key
            for (String recordKeyField : recordKeyFields) {
                // Extract value for this key
                Domain domain = domains.get(recordKeyField);
                List<String> values = extractStringValues(domain);
                // First iteration: initialize the queue
                if (results.isEmpty()) {
                    values.forEach(v -> results.offer(recordKeyField + DEFAULT_COLUMN_VALUE_SEPARATOR + v));
                }
                else {
                    int size = results.size();
                    for (int j = 0; j < size; j++) {
                        String currentEntry = results.poll();

                        // Generate new combinations by appending keyParts to existing keyParts
                        for (String v : values) {
                            String newKeyPart = recordKeyField + DEFAULT_COLUMN_VALUE_SEPARATOR + v;
                            String newEntry = currentEntry + DEFAULT_RECORD_KEY_PARTS_SEPARATOR + newKeyPart;
                            results.offer(newEntry);
                        }
                    }
                }
            }
            return results.stream().toList();
        }
    }

    /**
     * Extract string values from a domain, handle EQUAL and IN domains only.
     * Note: Actual implementation depends on your Domain class structure.
     */
    private static List<String> extractStringValues(Domain domain)
    {
        List<String> values = new ArrayList<>();

        if (domain.isSingleValue()) {
            // Handle EQUAL condition (single value domain)
            Object value = domain.getSingleValue();
            values.add(convertToString(value));
        }
        else if (domain.getValues().isDiscreteSet()) {
            // Handle IN condition (set of discrete values)
            for (Object value : domain.getValues().getDiscreteSet()) {
                values.add(convertToString(value));
            }
        }
        return values;
    }

    private static String convertToString(Object value)
    {
        if (value instanceof Slice) {
            return ((Slice) value).toStringUtf8();
        }
        else {
            return value.toString();
        }
    }

    public static boolean isIndexSupportAvailable(HoodieTableMetaClient metaClient)
    {
        return metaClient.getTableConfig().getMetadataPartitions().contains(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX);
    }
}
