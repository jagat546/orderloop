package app.orderloop.orchestrator.saga;

import app.orderloop.dto.SagaCommand;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * What {@link SagaEngine#decide} wants done. The application layer applies it in one transaction:
 * update the saga row, log the step, write the commands to the outbox, replace the deadline, open
 * the finding, and open an exception when the next state is {@code NeedsAttention}.
 *
 * <p>An ignored transition changes nothing, including the deadline; it is only counted.
 *
 * @param deadline the one deadline in force after this transition; empty cancels any
 * @param early the saga moved ahead on a provider event, so the skipped steps were observed
 * @param keepsDeadline the deadline already in force stays as it is: the input was ignored, or it
 *     only recorded a fact on the current step, such as an early settlement
 */
public record Transition(
    SagaState next,
    List<SagaCommand> commands,
    Optional<Deadline> deadline,
    Optional<Finding> finding,
    boolean early,
    boolean keepsDeadline,
    boolean ignored) {

  public Transition {
    Objects.requireNonNull(next, "next");
    commands = List.copyOf(commands);
  }

  /**
   * The one deadline in force after this transition; empty cancels any.
   *
   * @throws IllegalStateException for a transition that keeps the deadline as it is, ignored or
   *     not; reading it as "cancel" would leave the saga unable to time out
   */
  @Override
  public Optional<Deadline> deadline() {
    if (keepsDeadline) {
      throw new IllegalStateException("This transition keeps the deadline in force");
    }
    return deadline;
  }

  /** Enters a state, with the deadline that state runs under. */
  static Transition enter(SagaState next, Instant now) {
    Optional<Deadline> deadline =
        DeadlineKind.of(next).map(kind -> new Deadline(kind, now.plus(kind.duration())));
    return new Transition(next, List.of(), deadline, Optional.empty(), false, false, false);
  }

  /** Records a fact on the current step: the state's data changes, its deadline does not. */
  static Transition stay(SagaState next) {
    return new Transition(next, List.of(), Optional.empty(), Optional.empty(), false, true, false);
  }

  /** A late, duplicate or stale input: nothing changes. */
  static Transition ignore(SagaState current) {
    return new Transition(
        current, List.of(), Optional.empty(), Optional.empty(), false, true, true);
  }

  Transition emit(SagaCommand command) {
    List<SagaCommand> more = new ArrayList<>(commands);
    more.add(command);
    return new Transition(next, more, deadline, finding, early, keepsDeadline, ignored);
  }

  Transition open(Finding opened) {
    return new Transition(
        next, commands, deadline, Optional.of(opened), early, keepsDeadline, ignored);
  }

  Transition markEarly() {
    return new Transition(next, commands, deadline, finding, true, keepsDeadline, ignored);
  }
}
