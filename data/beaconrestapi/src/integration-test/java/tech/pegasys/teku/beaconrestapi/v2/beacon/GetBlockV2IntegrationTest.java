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

package tech.pegasys.teku.beaconrestapi.v2.beacon;

import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;
import static tech.pegasys.teku.infrastructure.http.ContentTypes.OCTET_STREAM;
import static tech.pegasys.teku.infrastructure.http.RestApiConstants.HEADER_CONSENSUS_VERSION;

import java.io.IOException;
import java.util.List;
import okhttp3.Response;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import tech.pegasys.teku.api.response.v2.beacon.GetBlockResponseV2;
import tech.pegasys.teku.api.schema.SignedBeaconBlock;
import tech.pegasys.teku.api.schema.Version;
import tech.pegasys.teku.api.schema.altair.SignedBeaconBlockAltair;
import tech.pegasys.teku.api.schema.phase0.SignedBeaconBlockPhase0;
import tech.pegasys.teku.beaconrestapi.AbstractDataBackedRestAPIIntegrationTest;
import tech.pegasys.teku.beaconrestapi.handlers.v2.beacon.GetBlock;
import tech.pegasys.teku.infrastructure.async.SafeFuture;
import tech.pegasys.teku.infrastructure.http.ContentTypes;
import tech.pegasys.teku.infrastructure.unsigned.UInt64;
import tech.pegasys.teku.spec.SpecMilestone;
import tech.pegasys.teku.spec.datastructures.blocks.SignedBlockAndState;
import tech.pegasys.teku.validator.coordinator.MissingDepositsException;

public class GetBlockV2IntegrationTest extends AbstractDataBackedRestAPIIntegrationTest {
  @Test
  public void shouldGetBlock() throws IOException {
    startRestAPIAtGenesis(SpecMilestone.PHASE0);
    final List<SignedBlockAndState> created = createBlocksAtSlots(10);
    final Response response = get("head");

    final GetBlockResponseV2 body =
        jsonProvider.jsonToObject(response.body().string(), GetBlockResponseV2.class);

    assertThat(body.getVersion()).isEqualTo(Version.phase0);
    assertThat(body.data).isInstanceOf(SignedBeaconBlockPhase0.class);
    final SignedBeaconBlockPhase0 data = (SignedBeaconBlockPhase0) body.getData();
    final SignedBlockAndState block = created.get(0);
    assertThat(data).isEqualTo(SignedBeaconBlock.create(block.getBlock()));

    assertThat(response.header(HEADER_CONSENSUS_VERSION)).isEqualTo(Version.phase0.name());
  }

  @Test
  public void shouldGetAltairBlock() throws IOException {
    startRestAPIAtGenesis(SpecMilestone.ALTAIR);
    final List<SignedBlockAndState> created = createBlocksAtSlots(10);
    final Response response = get("head");

    final GetBlockResponseV2 body =
        jsonProvider.jsonToObject(response.body().string(), GetBlockResponseV2.class);

    assertThat(body.getVersion()).isEqualTo(Version.altair);
    assertThat(body.getData()).isInstanceOf(SignedBeaconBlockAltair.class);
    final SignedBeaconBlockAltair data = (SignedBeaconBlockAltair) body.getData();
    assertThat(data.signature.toHexString())
        .isEqualTo(created.get(0).getBlock().getSignature().toString());
    assertThat(data.getMessage().asInternalBeaconBlock(spec).getRoot().toHexString())
        .isEqualTo(created.get(0).getBlock().getMessage().getRoot().toHexString());
    assertThat(data.getMessage().getBody().syncAggregate.syncCommitteeBits)
        .isEqualTo(Bytes.fromHexString("0x00000000"));
    assertThat(response.header(HEADER_CONSENSUS_VERSION)).isEqualTo(Version.altair.name());
  }

  @Test
  public void shouldGetAltairBlockAsSsz() throws IOException {
    startRestAPIAtGenesis(SpecMilestone.ALTAIR);
    createBlocksAtSlots(10);
    final Response response = get("head", OCTET_STREAM);
    assertThat(response.code()).isEqualTo(SC_OK);
    assertThat(response.header(HEADER_CONSENSUS_VERSION)).isEqualTo(Version.altair.name());
  }

  @Test
  public void shouldNotShowStackTraceForMissingDeposit() throws IOException {
    startRestAPIAtGenesis(SpecMilestone.ALTAIR);
    createBlocksAtSlots(10);
    // final DataStructureUtil dataStructureUtil = new DataStructureUtil(spec);
    // final BeaconBlock randomBlock = dataStructureUtil.randomBeaconBlock(UInt64.ONE);
    // final BLSSignature signature = randomBlock.getBody().getRandaoReveal();
    System.out.println("[SAM] we're in the bean hole");
    System.out.println(validatorApiChannel.toString());
    when(validatorApiChannel.createUnsignedBlock(any(), any(), any(), anyBoolean()))
        .thenReturn(
            SafeFuture.failedFuture(
                MissingDepositsException.missingRange(UInt64.valueOf(1), UInt64.valueOf(3))));
    final Response response = get("head", ContentTypes.JSON);
    System.out.println("[SAM] Response Body: " + response.body().string());
    // TODO: should be 500
    System.out.println("[SAM] Error code " + response.code());
    // System.out.println(response.code());
    System.out.println("[SAM] RESPONSE BODY " + response.body().string());
    assertThat(response.code()).isEqualTo(500);
    // System.out.println(response.body().string());
    // TODO(sragss): Make sure theres no stack trace in there
  }

  public Response get(final String blockIdString, final String contentType) throws IOException {
    System.out.println("[SAM] Calling: " + GetBlock.ROUTE.replace("{block_id}", blockIdString));
    return getResponse(GetBlock.ROUTE.replace("{block_id}", blockIdString), contentType);
  }

  public Response get(final String blockIdString) throws IOException {
    return getResponse(GetBlock.ROUTE.replace("{block_id}", blockIdString));
  }
}
