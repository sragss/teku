/*
 * Copyright ConsenSys Software Inc., 2022
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package tech.pegasys.teku.storage.server.kvstore;

import static com.google.common.base.Preconditions.checkNotNull;
import static tech.pegasys.teku.infrastructure.logging.StatusLogger.STATUS_LOG;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.MustBeClosed;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import tech.pegasys.teku.dataproviders.lookup.BlockProvider;
import tech.pegasys.teku.ethereum.pow.api.DepositsFromBlockEvent;
import tech.pegasys.teku.ethereum.pow.api.MinGenesisTimeBlockEvent;
import tech.pegasys.teku.infrastructure.async.SafeFuture;
import tech.pegasys.teku.infrastructure.unsigned.UInt64;
import tech.pegasys.teku.spec.Spec;
import tech.pegasys.teku.spec.datastructures.blocks.BeaconBlockHeader;
import tech.pegasys.teku.spec.datastructures.blocks.BeaconBlockSummary;
import tech.pegasys.teku.spec.datastructures.blocks.BlockAndCheckpointEpochs;
import tech.pegasys.teku.spec.datastructures.blocks.CheckpointEpochs;
import tech.pegasys.teku.spec.datastructures.blocks.SignedBeaconBlock;
import tech.pegasys.teku.spec.datastructures.blocks.SlotAndBlockRoot;
import tech.pegasys.teku.spec.datastructures.blocks.StateAndBlockSummary;
import tech.pegasys.teku.spec.datastructures.execution.ExecutionPayload;
import tech.pegasys.teku.spec.datastructures.execution.SlotAndExecutionPayload;
import tech.pegasys.teku.spec.datastructures.forkchoice.VoteTracker;
import tech.pegasys.teku.spec.datastructures.hashtree.HashTree;
import tech.pegasys.teku.spec.datastructures.state.AnchorPoint;
import tech.pegasys.teku.spec.datastructures.state.Checkpoint;
import tech.pegasys.teku.spec.datastructures.state.beaconstate.BeaconState;
import tech.pegasys.teku.storage.api.OnDiskStoreData;
import tech.pegasys.teku.storage.api.StorageUpdate;
import tech.pegasys.teku.storage.api.StoredBlockMetadata;
import tech.pegasys.teku.storage.api.UpdateResult;
import tech.pegasys.teku.storage.api.WeakSubjectivityState;
import tech.pegasys.teku.storage.api.WeakSubjectivityUpdate;
import tech.pegasys.teku.storage.server.Database;
import tech.pegasys.teku.storage.server.StateStorageMode;
import tech.pegasys.teku.storage.server.kvstore.dataaccess.CombinedKvStoreDao;
import tech.pegasys.teku.storage.server.kvstore.dataaccess.KvStoreCombinedDao;
import tech.pegasys.teku.storage.server.kvstore.dataaccess.KvStoreCombinedDao.CombinedUpdater;
import tech.pegasys.teku.storage.server.kvstore.dataaccess.KvStoreCombinedDaoAdapter;
import tech.pegasys.teku.storage.server.kvstore.dataaccess.KvStoreFinalizedDao.FinalizedUpdater;
import tech.pegasys.teku.storage.server.kvstore.dataaccess.KvStoreHotDao.HotUpdater;
import tech.pegasys.teku.storage.server.kvstore.dataaccess.V4FinalizedKvStoreDao;
import tech.pegasys.teku.storage.server.kvstore.dataaccess.V4FinalizedStateSnapshotStorageLogic;
import tech.pegasys.teku.storage.server.kvstore.dataaccess.V4FinalizedStateStorageLogic;
import tech.pegasys.teku.storage.server.kvstore.dataaccess.V4FinalizedStateTreeStorageLogic;
import tech.pegasys.teku.storage.server.kvstore.dataaccess.V4HotKvStoreDao;
import tech.pegasys.teku.storage.server.kvstore.schema.SchemaCombined;
import tech.pegasys.teku.storage.server.kvstore.schema.SchemaCombinedSnapshotState;
import tech.pegasys.teku.storage.server.kvstore.schema.SchemaCombinedTreeState;
import tech.pegasys.teku.storage.server.kvstore.schema.SchemaFinalizedSnapshotStateAdapter;
import tech.pegasys.teku.storage.server.kvstore.schema.SchemaHotAdapter;
import tech.pegasys.teku.storage.server.state.StateRootRecorder;

public class KvStoreDatabase implements Database {

  private static final Logger LOG = LogManager.getLogger();

  private static final int TX_BATCH_SIZE = 500;

  private final StateStorageMode stateStorageMode;

  private final Spec spec;
  private final boolean storeNonCanonicalBlocks;
  @VisibleForTesting final KvStoreCombinedDao dao;

  @SuppressWarnings("unused")
  private boolean storeBlockExecutionPayloadSeparately;

  public static Database createV4(
      final KvStoreAccessor hotDb,
      final KvStoreAccessor finalizedDb,
      final SchemaHotAdapter schemaHot,
      final SchemaFinalizedSnapshotStateAdapter schemaFinalized,
      final StateStorageMode stateStorageMode,
      final long stateStorageFrequency,
      final boolean storeNonCanonicalBlocks,
      final boolean storeBlockExecutionPayloadSeparately,
      final Spec spec) {
    final V4FinalizedStateSnapshotStorageLogic<SchemaFinalizedSnapshotStateAdapter>
        finalizedStateStorageLogic =
            new V4FinalizedStateSnapshotStorageLogic<>(stateStorageFrequency);
    final V4HotKvStoreDao hotDao = new V4HotKvStoreDao(hotDb, schemaHot);
    final KvStoreCombinedDao dao =
        new KvStoreCombinedDaoAdapter(
            hotDao,
            new V4FinalizedKvStoreDao(finalizedDb, schemaFinalized, finalizedStateStorageLogic));
    return new KvStoreDatabase(
        dao, stateStorageMode, storeNonCanonicalBlocks, storeBlockExecutionPayloadSeparately, spec);
  }

  public static Database createWithStateSnapshots(
      final KvStoreAccessor db,
      final SchemaCombinedSnapshotState schema,
      final StateStorageMode stateStorageMode,
      final long stateStorageFrequency,
      final boolean storeNonCanonicalBlocks,
      final boolean storeBlockExecutionPayloadSeparately,
      final Spec spec) {
    final V4FinalizedStateSnapshotStorageLogic<SchemaCombinedSnapshotState>
        finalizedStateStorageLogic =
            new V4FinalizedStateSnapshotStorageLogic<>(stateStorageFrequency);
    return create(
        db,
        schema,
        stateStorageMode,
        storeNonCanonicalBlocks,
        storeBlockExecutionPayloadSeparately,
        spec,
        finalizedStateStorageLogic);
  }

  public static Database createWithStateTree(
      final MetricsSystem metricsSystem,
      final KvStoreAccessor db,
      final SchemaCombinedTreeState schema,
      final StateStorageMode stateStorageMode,
      final boolean storeNonCanonicalBlocks,
      final boolean storeBlockExecutionPayloadSeparately,
      final int maxKnownNodeCacheSize,
      final Spec spec) {
    final V4FinalizedStateStorageLogic<SchemaCombinedTreeState> finalizedStateStorageLogic =
        new V4FinalizedStateTreeStorageLogic(metricsSystem, spec, maxKnownNodeCacheSize);
    return create(
        db,
        schema,
        stateStorageMode,
        storeNonCanonicalBlocks,
        storeBlockExecutionPayloadSeparately,
        spec,
        finalizedStateStorageLogic);
  }

  private static <S extends SchemaCombined> KvStoreDatabase create(
      final KvStoreAccessor db,
      final S schema,
      final StateStorageMode stateStorageMode,
      final boolean storeNonCanonicalBlocks,
      final boolean storeBlockExecutionPayloadSeparately,
      final Spec spec,
      final V4FinalizedStateStorageLogic<S> finalizedStateStorageLogic) {
    final CombinedKvStoreDao<S> dao =
        new CombinedKvStoreDao<S>(db, schema, finalizedStateStorageLogic);
    return new KvStoreDatabase(
        dao, stateStorageMode, storeNonCanonicalBlocks, storeBlockExecutionPayloadSeparately, spec);
  }

  private KvStoreDatabase(
      final KvStoreCombinedDao dao,
      final StateStorageMode stateStorageMode,
      final boolean storeNonCanonicalBlocks,
      final boolean storeBlockExecutionPayloadSeparately,
      final Spec spec) {
    this.dao = dao;
    this.storeBlockExecutionPayloadSeparately = storeBlockExecutionPayloadSeparately;
    checkNotNull(spec);
    this.stateStorageMode = stateStorageMode;
    this.storeNonCanonicalBlocks = storeNonCanonicalBlocks;
    this.spec = spec;
  }

  @Override
  public void storeInitialAnchor(final AnchorPoint anchor) {
    try (final CombinedUpdater updater = dao.combinedUpdater()) {
      // We should only have a single block / state / checkpoint at anchorpoint initialization
      final Checkpoint anchorCheckpoint = anchor.getCheckpoint();
      final Bytes32 anchorRoot = anchorCheckpoint.getRoot();
      final BeaconState anchorState = anchor.getState();
      final Optional<SignedBeaconBlock> anchorBlock = anchor.getSignedBeaconBlock();

      updater.setAnchor(anchor.getCheckpoint());
      updater.setGenesisTime(anchorState.getGenesisTime());
      updater.setJustifiedCheckpoint(anchorCheckpoint);
      updater.setBestJustifiedCheckpoint(anchorCheckpoint);
      updater.setFinalizedCheckpoint(anchorCheckpoint);
      updater.setLatestFinalizedState(anchorState);

      // We need to store the anchor block in both hot and cold storage so that on restart
      // we're guaranteed to have at least one block / state to load into RecentChainData.
      anchorBlock.ifPresent(
          block -> {
            // Save to hot storage
            updater.addHotBlock(
                new BlockAndCheckpointEpochs(
                    block,
                    new CheckpointEpochs(
                        anchorState.getCurrentJustifiedCheckpoint().getEpoch(),
                        anchorState.getFinalizedCheckpoint().getEpoch())));
            // Save to cold storage
            updater.addFinalizedBlock(block);
          });

      putFinalizedState(updater, anchorRoot, anchorState);

      updater.commit();
    }
  }

  @Override
  public UpdateResult update(final StorageUpdate event) {
    if (event.isEmpty()) {
      return UpdateResult.EMPTY;
    }
    return doUpdate(event);
  }

  public void ingestDatabase(
      final KvStoreDatabase kvStoreDatabase, final int batchSize, final Consumer<String> logger) {
    dao.ingest(kvStoreDatabase.dao, batchSize, logger);
  }

  @Override
  public void storeFinalizedBlocks(final Collection<SignedBeaconBlock> blocks) {
    if (blocks.isEmpty()) {
      return;
    }

    // Sort blocks and verify that they are contiguous with the oldestBlock
    final List<SignedBeaconBlock> sorted =
        blocks.stream()
            .sorted(Comparator.comparing(SignedBeaconBlock::getSlot).reversed())
            .collect(Collectors.toList());

    // The new block should be just prior to our earliest block if available, and otherwise should
    // match our latest finalized block
    Bytes32 expectedRoot =
        getEarliestAvailableBlock()
            .map(SignedBeaconBlock::getParentRoot)
            .orElseGet(() -> this.getLatestFinalizedBlockSummary().getRoot());
    for (SignedBeaconBlock block : sorted) {
      if (!block.getRoot().equals(expectedRoot)) {
        throw new IllegalArgumentException(
            "Blocks must be contiguous with the earliest known block.");
      }
      expectedRoot = block.getParentRoot();
    }

    try (final FinalizedUpdater updater = dao.finalizedUpdater()) {
      blocks.forEach(updater::addFinalizedBlock);
      updater.commit();
    }
  }

  @Override
  public void updateWeakSubjectivityState(WeakSubjectivityUpdate weakSubjectivityUpdate) {
    try (final HotUpdater updater = dao.hotUpdater()) {
      Optional<Checkpoint> checkpoint = weakSubjectivityUpdate.getWeakSubjectivityCheckpoint();
      checkpoint.ifPresentOrElse(
          updater::setWeakSubjectivityCheckpoint, updater::clearWeakSubjectivityCheckpoint);
      updater.commit();
    }
  }

  @Override
  public Optional<OnDiskStoreData> createMemoryStore() {
    return createMemoryStore(() -> Instant.now().getEpochSecond());
  }

  @VisibleForTesting
  Optional<OnDiskStoreData> createMemoryStore(final Supplier<Long> timeSupplier) {
    Optional<UInt64> maybeGenesisTime = dao.getGenesisTime();
    if (maybeGenesisTime.isEmpty()) {
      // If genesis time hasn't been set, genesis hasn't happened and we have no data
      return Optional.empty();
    }
    final UInt64 genesisTime = maybeGenesisTime.get();
    final Optional<Checkpoint> maybeAnchor = dao.getAnchor();
    final Checkpoint justifiedCheckpoint = dao.getJustifiedCheckpoint().orElseThrow();
    final Checkpoint finalizedCheckpoint = dao.getFinalizedCheckpoint().orElseThrow();
    final Checkpoint bestJustifiedCheckpoint = dao.getBestJustifiedCheckpoint().orElseThrow();
    final BeaconState finalizedState = dao.getLatestFinalizedState().orElseThrow();

    final Map<UInt64, VoteTracker> votes = dao.getVotes();

    // Build map with block information
    final Map<Bytes32, StoredBlockMetadata> blockInformation = new HashMap<>();
    try (final Stream<SignedBeaconBlock> hotBlocks = dao.streamHotBlocks()) {
      hotBlocks.forEach(
          b -> {
            final Optional<CheckpointEpochs> checkpointEpochs =
                dao.getHotBlockCheckpointEpochs(b.getRoot());
            blockInformation.put(
                b.getRoot(),
                new StoredBlockMetadata(
                    b.getSlot(),
                    b.getRoot(),
                    b.getParentRoot(),
                    b.getStateRoot(),
                    b.getMessage()
                        .getBody()
                        .getOptionalExecutionPayload()
                        .map(ExecutionPayload::getBlockHash),
                    checkpointEpochs));
          });
    }
    // If anchor block is missing, try to pull block info from the anchor state
    final boolean shouldIncludeAnchorBlock =
        maybeAnchor.isPresent()
            && finalizedCheckpoint
                .getEpochStartSlot(spec)
                .equals(maybeAnchor.get().getEpochStartSlot(spec));
    if (shouldIncludeAnchorBlock && !blockInformation.containsKey(maybeAnchor.get().getRoot())) {
      final Checkpoint anchor = maybeAnchor.orElseThrow();
      final StateAndBlockSummary latestFinalized = StateAndBlockSummary.create(finalizedState);
      if (!latestFinalized.getRoot().equals(anchor.getRoot())) {
        throw new IllegalStateException("Anchor state (" + anchor + ") is unavailable");
      }
      blockInformation.put(
          anchor.getRoot(), StoredBlockMetadata.fromBlockAndState(latestFinalized));
    }

    final Optional<SignedBeaconBlock> finalizedBlock =
        dao.getHotBlock(finalizedCheckpoint.getRoot());
    final AnchorPoint latestFinalized =
        AnchorPoint.create(spec, finalizedCheckpoint, finalizedState, finalizedBlock);
    final Optional<SlotAndExecutionPayload> finalizedOptimisticTransitionPayload =
        dao.getOptimisticTransitionBlockSlot()
            .flatMap(dao::getFinalizedBlockAtSlot)
            .flatMap(SlotAndExecutionPayload::fromBlock);

    // Make sure time is set to a reasonable value in the case where we start up before genesis when
    // the clock time would be prior to genesis
    final long clockTime = timeSupplier.get();
    final UInt64 slotTime = spec.getSlotStartTime(finalizedState.getSlot(), genesisTime);
    final UInt64 time = slotTime.max(clockTime);

    return Optional.of(
        new OnDiskStoreData(
            time,
            maybeAnchor,
            genesisTime,
            latestFinalized,
            finalizedOptimisticTransitionPayload,
            justifiedCheckpoint,
            bestJustifiedCheckpoint,
            blockInformation,
            votes));
  }

  @Override
  public WeakSubjectivityState getWeakSubjectivityState() {
    return WeakSubjectivityState.create(dao.getWeakSubjectivityCheckpoint());
  }

  @Override
  public Map<UInt64, VoteTracker> getVotes() {
    return dao.getVotes();
  }

  @Override
  public Optional<UInt64> getSlotForFinalizedBlockRoot(final Bytes32 blockRoot) {
    return dao.getSlotForFinalizedBlockRoot(blockRoot);
  }

  @Override
  public Optional<UInt64> getSlotForFinalizedStateRoot(final Bytes32 stateRoot) {
    return dao.getSlotForFinalizedStateRoot(stateRoot);
  }

  @Override
  public Optional<SignedBeaconBlock> getFinalizedBlockAtSlot(final UInt64 slot) {
    return dao.getFinalizedBlockAtSlot(slot);
  }

  @Override
  public Optional<UInt64> getEarliestAvailableBlockSlot() {
    return dao.getEarliestFinalizedBlockSlot();
  }

  @Override
  public Optional<SignedBeaconBlock> getEarliestAvailableBlock() {
    return dao.getEarliestFinalizedBlock();
  }

  @Override
  public Optional<SignedBeaconBlock> getLatestFinalizedBlockAtSlot(final UInt64 slot) {
    return dao.getLatestFinalizedBlockAtSlot(slot);
  }

  @Override
  public Optional<BeaconState> getLatestAvailableFinalizedState(final UInt64 maxSlot) {
    return dao.getLatestAvailableFinalizedState(maxSlot);
  }

  @Override
  public Optional<SignedBeaconBlock> getSignedBlock(final Bytes32 root) {
    return dao.getHotBlock(root)
        .or(() -> dao.getFinalizedBlock(root))
        .or(() -> dao.getNonCanonicalBlock(root));
  }

  @Override
  public Optional<BeaconState> getHotState(final Bytes32 root) {
    return dao.getHotState(root);
  }

  @Override
  public Map<Bytes32, SignedBeaconBlock> getHotBlocks(final Set<Bytes32> blockRoots) {
    return blockRoots.stream()
        .flatMap(root -> dao.getHotBlock(root).stream())
        .collect(Collectors.toMap(SignedBeaconBlock::getRoot, Function.identity()));
  }

  @Override
  public Optional<SignedBeaconBlock> getHotBlock(final Bytes32 blockRoot) {
    return dao.getHotBlock(blockRoot);
  }

  @Override
  @MustBeClosed
  public Stream<SignedBeaconBlock> streamFinalizedBlocks(
      final UInt64 startSlot, final UInt64 endSlot) {
    return dao.streamFinalizedBlocks(startSlot, endSlot);
  }

  @Override
  @MustBeClosed
  public Stream<SignedBeaconBlock> streamHotBlocks() {
    return dao.streamHotBlocks();
  }

  @Override
  @MustBeClosed
  public Stream<Map.Entry<Bytes32, CheckpointEpochs>> streamCheckpointEpochs() {
    return dao.streamCheckpointEpochs();
  }

  @Override
  public List<Bytes32> getStateRootsBeforeSlot(final UInt64 slot) {
    return dao.getStateRootsBeforeSlot(slot);
  }

  @Override
  public void addHotStateRoots(
      final Map<Bytes32, SlotAndBlockRoot> stateRootToSlotAndBlockRootMap) {
    try (final HotUpdater updater = dao.hotUpdater()) {
      updater.addHotStateRoots(stateRootToSlotAndBlockRootMap);
      updater.commit();
    }
  }

  @Override
  public Optional<SlotAndBlockRoot> getSlotAndBlockRootFromStateRoot(final Bytes32 stateRoot) {
    Optional<SlotAndBlockRoot> maybeSlotAndBlockRoot =
        dao.getSlotAndBlockRootFromStateRoot(stateRoot);
    if (maybeSlotAndBlockRoot.isPresent()) {
      return maybeSlotAndBlockRoot;
    }
    return dao.getSlotAndBlockRootForFinalizedStateRoot(stateRoot);
  }

  @Override
  public void pruneHotStateRoots(final List<Bytes32> stateRoots) {
    try (final HotUpdater updater = dao.hotUpdater()) {
      updater.pruneHotStateRoots(stateRoots);
      updater.commit();
    }
  }

  @Override
  public Optional<MinGenesisTimeBlockEvent> getMinGenesisTimeBlock() {
    return dao.getMinGenesisTimeBlock();
  }

  @Override
  public List<SignedBeaconBlock> getNonCanonicalBlocksAtSlot(final UInt64 slot) {
    return dao.getNonCanonicalBlocksAtSlot(slot);
  }

  @Override
  @MustBeClosed
  public Stream<DepositsFromBlockEvent> streamDepositsFromBlocks() {
    return dao.streamDepositsFromBlocks();
  }

  @Override
  public void addMinGenesisTimeBlock(final MinGenesisTimeBlockEvent event) {
    try (final HotUpdater updater = dao.hotUpdater()) {
      updater.addMinGenesisTimeBlock(event);
      updater.commit();
    }
  }

  @Override
  public void addDepositsFromBlockEvent(final DepositsFromBlockEvent event) {
    try (final HotUpdater updater = dao.hotUpdater()) {
      updater.addDepositsFromBlockEvent(event);
      updater.commit();
    }
  }

  @Override
  public void storeVotes(final Map<UInt64, VoteTracker> votes) {
    try (final HotUpdater hotUpdater = dao.hotUpdater()) {
      hotUpdater.addVotes(votes);
      hotUpdater.commit();
    }
  }

  @Override
  public void close() throws Exception {
    dao.close();
  }

  private UpdateResult doUpdate(final StorageUpdate update) {
    LOG.trace("Applying finalized updates");
    // Update finalized blocks and states
    final Optional<SlotAndExecutionPayload> finalizedOptimisticExecutionPayload =
        updateFinalizedData(
            update.getFinalizedChildToParentMap(),
            update.getFinalizedBlocks(),
            update.getFinalizedStates(),
            update.getDeletedHotBlocks(),
            update.isFinalizedOptimisticTransitionBlockRootSet(),
            update.getOptimisticTransitionBlockRoot());
    LOG.trace("Applying hot updates");
    try (final HotUpdater updater = dao.hotUpdater()) {
      // Store new hot data
      update.getGenesisTime().ifPresent(updater::setGenesisTime);
      update
          .getFinalizedCheckpoint()
          .ifPresent(
              checkpoint -> {
                updater.setFinalizedCheckpoint(checkpoint);
                final int slotsPerEpoch = spec.slotsPerEpoch(checkpoint.getEpoch());
                final UInt64 finalizedSlot = checkpoint.getEpochStartSlot(spec).plus(slotsPerEpoch);
                updater.pruneHotStateRoots(dao.getStateRootsBeforeSlot(finalizedSlot));
                updater.deleteHotState(checkpoint.getRoot());
              });

      update.getJustifiedCheckpoint().ifPresent(updater::setJustifiedCheckpoint);
      update.getBestJustifiedCheckpoint().ifPresent(updater::setBestJustifiedCheckpoint);
      update.getLatestFinalizedState().ifPresent(updater::setLatestFinalizedState);

      updater.addHotBlocks(update.getHotBlocks());
      updater.addHotStates(update.getHotStates());

      if (update.getStateRoots().size() > 0) {
        updater.addHotStateRoots(update.getStateRoots());
      }

      // Delete finalized data from hot db
      update.getDeletedHotBlocks().forEach(updater::deleteHotBlock);

      LOG.trace("Committing hot db changes");
      updater.commit();
    }
    LOG.trace("Update complete");
    return new UpdateResult(finalizedOptimisticExecutionPayload);
  }

  private Optional<SlotAndExecutionPayload> updateFinalizedData(
      Map<Bytes32, Bytes32> finalizedChildToParentMap,
      final Map<Bytes32, SignedBeaconBlock> finalizedBlocks,
      final Map<Bytes32, BeaconState> finalizedStates,
      final Set<Bytes32> deletedHotBlocks,
      final boolean isFinalizedOptimisticBlockRootSet,
      final Optional<Bytes32> finalizedOptimisticTransitionBlockRoot) {
    if (finalizedChildToParentMap.isEmpty()) {
      // Nothing to do
      return Optional.empty();
    }

    final Optional<SlotAndExecutionPayload> optimisticTransitionPayload =
        updateFinalizedOptimisticTransitionBlock(
            isFinalizedOptimisticBlockRootSet, finalizedOptimisticTransitionBlockRoot);
    switch (stateStorageMode) {
      case ARCHIVE:
        updateFinalizedDataArchiveMode(finalizedChildToParentMap, finalizedBlocks, finalizedStates);
        break;

      case PRUNE:
        updateFinalizedDataPruneMode(finalizedChildToParentMap, finalizedBlocks);
        break;
      default:
        throw new UnsupportedOperationException("Unhandled storage mode: " + stateStorageMode);
    }

    if (storeNonCanonicalBlocks) {
      storeNonCanonicalBlocks(
          deletedHotBlocks.stream()
              .filter(root -> !finalizedChildToParentMap.containsKey(root))
              .flatMap(root -> getHotBlock(root).stream())
              .collect(Collectors.toSet()));
    }

    return optimisticTransitionPayload;
  }

  private Optional<SlotAndExecutionPayload> updateFinalizedOptimisticTransitionBlock(
      final boolean isFinalizedOptimisticBlockRootSet,
      final Optional<Bytes32> finalizedOptimisticTransitionBlockRoot) {
    if (isFinalizedOptimisticBlockRootSet) {
      final Optional<SignedBeaconBlock> transitionBlock =
          finalizedOptimisticTransitionBlockRoot.flatMap(this::getHotBlock);
      try (final FinalizedUpdater updater = dao.finalizedUpdater()) {
        updater.setOptimisticTransitionBlockSlot(transitionBlock.map(SignedBeaconBlock::getSlot));
        updater.commit();
      }
      return transitionBlock.flatMap(SlotAndExecutionPayload::fromBlock);
    } else {
      return Optional.empty();
    }
  }

  private void storeNonCanonicalBlocks(final Set<SignedBeaconBlock> nonCanonicalBlocks) {
    int i = 0;
    final Iterator<SignedBeaconBlock> it = nonCanonicalBlocks.iterator();
    while (it.hasNext()) {
      final Map<UInt64, Set<Bytes32>> nonCanonicalRootsBySlotBuffer = new HashMap<>();
      final int start = i;
      try (final FinalizedUpdater updater = dao.finalizedUpdater()) {
        while (it.hasNext() && (i - start) < TX_BATCH_SIZE) {
          final SignedBeaconBlock block = it.next();
          LOG.debug("Non canonical block {}:{}", block.getRoot().toHexString(), block.getSlot());
          updater.addNonCanonicalBlock(block);
          nonCanonicalRootsBySlotBuffer
              .computeIfAbsent(block.getSlot(), __ -> new HashSet<>())
              .add(block.getRoot());
          i++;
        }
        nonCanonicalRootsBySlotBuffer.forEach(updater::addNonCanonicalRootAtSlot);
        updater.commit();
      }
    }
  }

  private void updateFinalizedDataArchiveMode(
      Map<Bytes32, Bytes32> finalizedChildToParentMap,
      final Map<Bytes32, SignedBeaconBlock> finalizedBlocks,
      final Map<Bytes32, BeaconState> finalizedStates) {
    final BlockProvider blockProvider =
        BlockProvider.withKnownBlocks(
            roots -> SafeFuture.completedFuture(getHotBlocks(roots)), finalizedBlocks);

    final Optional<Checkpoint> initialCheckpoint = dao.getAnchor();
    final Optional<Bytes32> initialBlockRoot = initialCheckpoint.map(Checkpoint::getRoot);
    // Get previously finalized block to build on top of
    final BeaconBlockSummary baseBlock = getLatestFinalizedBlockOrSummary();

    final List<Bytes32> finalizedRoots =
        HashTree.builder()
            .rootHash(baseBlock.getRoot())
            .childAndParentRoots(finalizedChildToParentMap)
            .build()
            .preOrderStream()
            .collect(Collectors.toList());

    int i = 0;
    UInt64 lastSlot = baseBlock.getSlot();
    while (i < finalizedRoots.size()) {
      final int start = i;
      try (final FinalizedUpdater updater = dao.finalizedUpdater()) {
        final StateRootRecorder recorder =
            new StateRootRecorder(lastSlot, updater::addFinalizedStateRoot, spec);

        while (i < finalizedRoots.size() && (i - start) < TX_BATCH_SIZE) {
          final Bytes32 blockRoot = finalizedRoots.get(i);

          final Optional<SignedBeaconBlock> maybeBlock = blockProvider.getBlock(blockRoot).join();
          maybeBlock.ifPresent(updater::addFinalizedBlock);
          // If block is missing and doesn't match the initial anchor, throw
          if (maybeBlock.isEmpty() && initialBlockRoot.filter(r -> r.equals(blockRoot)).isEmpty()) {
            throw new IllegalStateException("Missing finalized block");
          }

          Optional.ofNullable(finalizedStates.get(blockRoot))
              .or(() -> getHotState(blockRoot))
              .ifPresent(
                  state -> {
                    updater.addFinalizedState(blockRoot, state);
                    recorder.acceptNextState(state);
                  });

          lastSlot =
              maybeBlock
                  .map(SignedBeaconBlock::getSlot)
                  .orElseGet(() -> initialCheckpoint.orElseThrow().getEpochStartSlot(spec));
          i++;
        }
        updater.commit();
        if (i >= TX_BATCH_SIZE) {
          STATUS_LOG.recordedFinalizedBlocks(i, finalizedRoots.size());
        }
      }
    }
  }

  private void updateFinalizedDataPruneMode(
      Map<Bytes32, Bytes32> finalizedChildToParentMap,
      final Map<Bytes32, SignedBeaconBlock> finalizedBlocks) {
    final Optional<Bytes32> initialBlockRoot = dao.getAnchor().map(Checkpoint::getRoot);
    final BlockProvider blockProvider =
        BlockProvider.withKnownBlocks(
            roots -> SafeFuture.completedFuture(getHotBlocks(roots)), finalizedBlocks);

    final List<Bytes32> finalizedRoots = new ArrayList<>(finalizedChildToParentMap.keySet());
    int i = 0;
    while (i < finalizedRoots.size()) {
      try (final FinalizedUpdater updater = dao.finalizedUpdater()) {
        final int start = i;
        while (i < finalizedRoots.size() && (i - start) < TX_BATCH_SIZE) {
          final Bytes32 root = finalizedRoots.get(i);
          final Optional<SignedBeaconBlock> maybeBlock = blockProvider.getBlock(root).join();
          maybeBlock.ifPresent(updater::addFinalizedBlock);

          // If block is missing and doesn't match the initial anchor, throw
          if (maybeBlock.isEmpty() && initialBlockRoot.filter(r -> r.equals(root)).isEmpty()) {
            throw new IllegalStateException("Missing finalized block");
          }
          i++;
        }
        updater.commit();
        if (i >= TX_BATCH_SIZE) {
          STATUS_LOG.recordedFinalizedBlocks(i, finalizedRoots.size());
        }
      }
    }
  }

  private BeaconBlockSummary getLatestFinalizedBlockOrSummary() {
    final Bytes32 baseBlockRoot = dao.getFinalizedCheckpoint().orElseThrow().getRoot();
    return dao.getFinalizedBlock(baseBlockRoot)
        .<BeaconBlockSummary>map(a -> a)
        .orElseGet(this::getLatestFinalizedBlockSummary);
  }

  private BeaconBlockSummary getLatestFinalizedBlockSummary() {
    final Optional<BeaconBlockSummary> finalizedBlock =
        dao.getLatestFinalizedState().map(BeaconBlockHeader::fromState);
    return finalizedBlock.orElseThrow(
        () -> new IllegalStateException("Unable to reconstruct latest finalized block summary"));
  }

  private void putFinalizedState(
      FinalizedUpdater updater, final Bytes32 blockRoot, final BeaconState state) {
    switch (stateStorageMode) {
      case ARCHIVE:
        updater.addFinalizedState(blockRoot, state);
        break;
      case PRUNE:
        // Don't persist finalized state
        break;
      default:
        throw new UnsupportedOperationException("Unhandled storage mode: " + stateStorageMode);
    }
  }
}
