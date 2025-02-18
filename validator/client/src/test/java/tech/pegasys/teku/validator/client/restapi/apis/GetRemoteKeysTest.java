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

package tech.pegasys.teku.validator.client.restapi.apis;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static tech.pegasys.teku.core.signatures.NoOpRemoteSigner.NO_OP_REMOTE_SIGNER;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import tech.pegasys.teku.bls.BLSKeyPair;
import tech.pegasys.teku.bls.BLSTestUtil;
import tech.pegasys.teku.infrastructure.restapi.endpoints.RestApiRequest;
import tech.pegasys.teku.validator.client.ActiveKeyManager;
import tech.pegasys.teku.validator.client.Validator;
import tech.pegasys.teku.validator.client.restapi.apis.schema.ExternalValidator;

class GetRemoteKeysTest {
  final ActiveKeyManager keyManager = mock(ActiveKeyManager.class);

  @Test
  void shouldListValidatorKeys() throws Exception {
    final List<ExternalValidator> activeRemoteValidatorList =
        getValidatorList().stream()
            .map(
                val ->
                    new ExternalValidator(
                        val.getPublicKey(),
                        val.getSigner().getSigningServiceUrl(),
                        val.isReadOnly()))
            .collect(Collectors.toList());
    when(keyManager.getActiveRemoteValidatorKeys()).thenReturn(activeRemoteValidatorList);
    final GetRemoteKeys endpoint = new GetRemoteKeys(keyManager);
    final RestApiRequest request = mock(RestApiRequest.class);
    endpoint.handleRequest(request);

    verify(request).respondOk(activeRemoteValidatorList);
  }

  @Test
  void shouldListEmptyValidatorKeys() throws Exception {
    final List<ExternalValidator> activeRemoteValidatorList = Collections.emptyList();
    when(keyManager.getActiveRemoteValidatorKeys()).thenReturn(activeRemoteValidatorList);
    final GetRemoteKeys endpoint = new GetRemoteKeys(keyManager);
    final RestApiRequest request = mock(RestApiRequest.class);
    endpoint.handleRequest(request);

    verify(request).respondOk(Collections.emptyList());
  }

  private List<Validator> getValidatorList() {
    BLSKeyPair keyPair1 = BLSTestUtil.randomKeyPair(1);
    BLSKeyPair keyPair2 = BLSTestUtil.randomKeyPair(2);
    Validator validator1 =
        new Validator(keyPair1.getPublicKey(), NO_OP_REMOTE_SIGNER, Optional::empty, false);
    Validator validator2 =
        new Validator(keyPair2.getPublicKey(), NO_OP_REMOTE_SIGNER, Optional::empty, false);
    return Arrays.asList(validator1, validator2);
  }
}
