package com.amazon.janusgraph.diskstorage.dynamodb;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import java.util.function.Function;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;

import lombok.RequiredArgsConstructor;
import lombok.NonNull;
/**
 * Asynchronous task support. Delegates to a composed completable future.
 * @author Naveen Gattu
 *
 */

@RequiredArgsConstructor
public class AsyncTask<R> {

    @NonNull
    protected final CompletableFuture<R> cf;

    public AsyncTask() {
        this.cf = new CompletableFuture<R>();
    }

    /**
     * Returns a new AsyncTask that is already completed with
     * the given value.
     *
     * @param value the value
     * @param <U> the type of the value
     * @return the completed AsyncTask
     */
    public static <U> AsyncTask<U> completedFuture(final U value) {
        return new AsyncTask(CompletableFuture.completedFuture(value));
    }

    /**
    * Returns a new AsyncTask that is completed when all of
    * the given AsyncTasks complete.  If any of the given
    * AsyncTasks complete exceptionally, then the returned
    * AsyncTask also does so, with a CompletionException
    * holding this exception as its cause.  Otherwise, the results,
    * if any, of the given AsyncTasks are not reflected in
    * the returned AsyncTask, but may be obtained by
    * inspecting them individually. If no AsyncTasks are
    * provided, returns a AsyncTask completed with the value
    * {@code null}.
    *
    * <p>Among the applications of this method is to await completion
    * of a set of independent AsyncTasks before continuing a
    * program, as in: {@code AsyncTask.allOf(c1, c2,
    * c3).join();}.
    *
    * @param cfs the AsyncTasks
    * @return a new AsyncTask that is completed when all of the
    * given AsyncTasks complete
    * @throws NullPointerException if the array or any of its elements are
    * {@code null}
    */
    public static AsyncTask<Void> allOf(final AsyncTask<?>... ats) {
        return new AsyncTask(CompletableFuture.allOf(Stream.of(ats).map(at -> at.cf).toArray(CompletableFuture[]::new)));
    }

    public R get() throws BackendException {
        try {
            return cf.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BackendRuntimeException("operation was interrupted");
        } catch (CancellationException e) {
            throw new BackendRuntimeException("operation was cancelled");
        } catch (ExecutionException e) {
            Throwable exc = e.getCause();
            while (exc instanceof CompletionException) {
                exc = ((CompletionException) exc).getCause();
            }
            if (exc instanceof BackendException) {
                throw (BackendException) exc;
            } else {
                throw new PermanentBackendException(exc);
            }
        }
    }


    public <U> AsyncTask<U> map(final Function<? super R, ? extends U> fn) {
        return new AsyncTask(cf.thenApply(fn));
    }

    public <U> AsyncTask<U> flatMap(final Function<? super R, ? extends AsyncTask<U>> fn) {
        return new AsyncTask(cf.thenCompose(r -> {
            return fn.apply(r).cf;
        }));
    }

    public AsyncTask<R> whenComplete(final BiConsumer<? super R, ? super Throwable> action) {
        return new AsyncTask(cf.whenComplete((r, e) -> {
            while (e instanceof CompletionException) {
                e = ((CompletionException) e).getCause();
            }
            action.accept(r, e);
        }));
    }

    /**
     * If not already completed, sets the value returned by {@link
     * #get()} and related methods to the given value.
     *
     * @param value the result value
     * @return {@code true} if this invocation caused this AsyncTask
     * to transition to a completed state, else {@code false}
     */
    public boolean complete(final R value) {
        return cf.complete(value);
    }


    /**
     * If not already completed, causes invocations of {@link #get()}
     * and related methods to throw the given exception.
     *
     * @param ex the exception
     * @return {@code true} if this invocation caused this AsyncTask
     * to transition to a completed state, else {@code false}
     */
    public boolean completeExceptionally(final Throwable ex) {
        return cf.completeExceptionally(ex);
    }

    /**
    * Returns the result value when complete, or throws an
    * (unchecked) exception if completed exceptionally. To better
    * conform with the use of common functional forms, if a
    * computation involved in the completion of this
    * AsyncTask threw an exception, this method throws an
    * (unchecked) {@link CompletionException} with the underlying
    * exception as its cause.
    *
    * @return the result value
    * @throws CancellationException if the computation was cancelled
    * @throws CompletionException if this future completed
    * exceptionally or a completion computation threw an exception
    */
    @SuppressWarnings("unchecked")
    public R join() {
        return cf.join();
    }

    /**
    * If not already completed, completes this CompletableFuture with
    * a {@link CancellationException}. Dependent CompletableFutures
    * that have not already completed will also complete
    * exceptionally, with a {@link CompletionException} caused by
    * this {@code CancellationException}.
    *
    * @param mayInterruptIfRunning this value has no effect in this
    * implementation because interrupts are not used to control
    * processing.
    *
    * @return {@code true} if this task is now cancelled
    */
    public boolean cancel(final boolean mayInterruptIfRunning) {
        return cf.cancel(mayInterruptIfRunning);
    }
}

