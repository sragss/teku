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

package tech.pegasys.teku.beaconrestapi.v1.validator;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import tech.pegasys.teku.infrastructure.async.ExceptionThrowingRunnable;
import tech.pegasys.teku.infrastructure.async.SafeFuture;
import tech.pegasys.teku.infrastructure.unsigned.UInt64;
import tech.pegasys.teku.storage.client.ChainDataUnavailableException;
import tech.pegasys.teku.validator.coordinator.MissingDepositsException;

public class SafeFutureTest {

  @Test
  void shouldTestFutures() throws Exception {
    // Create a SafeFuture which returns an error after some delay
    System.out.println("[SAM] shouldTestFutures -- starting");
    final SafeFuture<Optional<Boolean>> optFuture = workingResult();

    // failingFuture.wait();
    assertThat(1).isEqualTo(1);
    System.out.println("[SAM] Future result " + optFuture.get().toString());
    System.out.println("[SAM] shouldTestFutures -- completing");
  }

  private SafeFuture<Optional<Boolean>> workingResult() {
    final SafeFuture<Void> failingFuture = SafeFuture.fromRunnable(new RunnableImpl());
    return failingFuture.thenApply(___ -> Optional.of(true));
  }

  @Test
  void testExceptions() throws Exception {
    final SafeFuture<Optional<Boolean>> throwFuture = throwingFuture();
    final SafeFuture<String> resultFuture = throwFuture.thenApply(result -> {
        // throw new Exception("meow ");
        if (!result.isPresent()) {
            throw new ChainDataUnavailableException();
        }
        return "GOT THE RESULT WOOHOO";
    }).exceptionally(err -> {
        if (err instanceof MissingDepositsException) {
            return err.getMessage();
        } else {
            throw new RuntimeException(err);
        }
    });

    // When it finishes, unwrap it, if unwrap fails, 
    System.out.println("[SAM] Failing future result " + resultFuture.get().toString());
  }

//   private SafeFuture<Optional<Boolean>> emptyResult() {
//     final SafeFuture<Void> failingFuture = SafeFuture.fromRunnable(new RunnableImpl());
//     return failingFuture.thenApply(___ -> Optional.of(true));
//   }

  private SafeFuture<Optional<Boolean>> throwingFuture() {
    final SafeFuture<Void> failingFuture = SafeFuture.fromRunnable(new ThrowingRunnableImpl());
    return failingFuture.thenApply(___ -> Optional.of(true));
  }

  public static class RunnableImpl implements ExceptionThrowingRunnable {
    @Override
    public void run() throws InterruptedException {
      for (int i = 0; i < 4; i++) {
        System.out.println("[SAM] RunnableImpl.run() " + i);
        TimeUnit.SECONDS.sleep(2);
      }
    }
  }

  public static class ThrowingRunnableImpl implements ExceptionThrowingRunnable {
    @Override
    public void run() throws InterruptedException, MissingDepositsException {
      for (int i = 0; i < 4; i++) {
        System.out.println("[SAM] ThrowingRunnableImpl.run() " + i);
        TimeUnit.SECONDS.sleep(2);
        if (i == 2) {
            // throw MissingDepositsException.missingRange(UInt64.valueOf(1), UInt64.valueOf(2));
            throw new ChainDataUnavailableException();
        }
      }
    }
  }
}
