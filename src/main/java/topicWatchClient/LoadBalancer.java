package topicWatchClient;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.common.protocol.types.Field;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.rowset.spi.SyncResolver;
import java.lang.invoke.WrongMethodTypeException;
import java.util.*;


/**
 * <b>Round-robin re-balancer Keeps track of, assigns, and balances jobs watched by a set of workers
 *
 * @see <a
 *     href="https://stackoverflow.com/questions/39489193/algorithms-for-rebalancing-round-robin-assignments">
 *     StackOverFlow Rebalancer</a> The implementation is in the spirit of the link above but not
 *     the exact implementation.
 *
 * Note: This is not thread safe. Could make it thread save with ReaderWriterLock on `CACHE` 
 */
public class LoadBalancer {

  private static final Logger LOG = LoggerFactory.getLogger(LoadBalancer.class);


  /** The `CACHE' is an array list of worker's */
  @VisibleForTesting volatile ArrayList<Worker> CACHE = new ArrayList<>();

  /** The state of round-robin algorithm will be the workerID that was last assigned a job */
  public Integer previousWorkerIdx;

  /** set of all jobs managed by LoadBalancer*/
  Set<String> allJobSet=new HashSet<>();

  /**
   * Returns a clone of the CACHE.
   */
  public ArrayList<Worker> getCACHE() {
    return new ArrayList<>(CACHE);
  }

  /**
   * Initialize the LoadBalancer with a collection of workerID's that uniquely identify the worker
   * thread instances to be monitored
   *
   * @param workerIds a collection of strings that uniquely identify a worker being managed by LoadBalancer
   */
  public LoadBalancer(Collection<String> workerIds) {
    // initialize the `CACHE`
    workerIds.forEach((workerId) -> CACHE.add(new Worker(workerId)));
    // at initialization the lastWorker is null since no jobs have been assigned.
    previousWorkerIdx = null;

    System.out.println("\t\t[LoadBalancer] Initialized");
  }

  public LoadBalancer(){
    // at initialization the lastWorker is null since no jobs have been assigned.
    previousWorkerIdx = null;

    System.out.println("\t\t[LoadBalancer] Initialized");
  }

  /**
   * Returns the jobSet given a workerID
   *
   * @param workerID the identifier of the worker instance
   * @return a jobSet
   */
  public Set<String> getJobSet(String workerID) {
    FindWorkerResult findWorkerResult;
    if ((findWorkerResult= findWorker(workerID))!=null){
      return findWorkerResult.worker.getJobSet();
    }else{
      return null;
    }

  }

  /**
   * Returns the number of workers in load balancer
   */
  public int getNumberOfWorkers(){
    return CACHE.size();
  }

  /**
   *  Returns the Worker given a workerID
   * @param workerID workerID of the worker to find
   * @return {@link Worker}
   */
  public Worker getWorker(String workerID){
    FindWorkerResult findWorkerResult;
    if ((findWorkerResult=findWorker(workerID))==null){
      return null;
    }else{
      return findWorkerResult.worker;
    }
  }

  /**
   * Checks if LoadBalancer has jobs
   * @return true if the global job set is empty otherwise false
   */
  private boolean hasNoJobs(){
    return allJobSet.isEmpty();
  }

  /**
   * Adds a worker to the load balancer
   *
   * @param workerID the workerID of the added worker
   * @return the set of workers that have been modified
   */
  public Set<Worker> addWorker(String workerID){
    if (workerID.isEmpty())
      return null;

    Worker addedWorker = new Worker(workerID);
    Set<Worker> addWorkerResultSet = new LinkedHashSet<>();

    // if CACHE hasNoJobs add the new worker and move on
    if (hasNoJobs()){
      CACHE.add(addedWorker);
      addWorkerResultSet.add(addedWorker);
      return addWorkerResultSet;
    }

    Worker firstWorker = CACHE.get(0);
    Worker previousWorker;
    String job;

    while (addedWorker.jobSet.size()< firstWorker.jobSet.size()-1){
      // transfer a job from `previousWorker' to `addedWorker'
      previousWorker = CACHE.get(previousWorkerIdx);
      job = previousWorker.getElementFromJobSet();
      previousWorker.removeJob(job);
      addedWorker.addJob(job);

      // a worker has changed so add it to the result set
      addWorkerResultSet.add(previousWorker);

      // decrement and repeat
      decrementPreviousWorkerIdx();
    }

    // add the worker with job
    CACHE.add(addedWorker);

    return addWorkerResultSet;
  }



  /**
   * Remove a worker and redistribute the removed workers jobs
   *
   * @param workerID the workerID of the worker to remove
   * @return a set of Workers that received new jobs as a result of the worker being removed
   */
  public Set<Worker> removeWorker(String workerID){
    if (workerID.isEmpty())
      return null;


    FindWorkerResult findWorkerResult;
    Worker workerToRemove;
    int workerToRemoveCacheIdx;

    if ((findWorkerResult=findWorker(workerID))!=null){
      workerToRemove = findWorkerResult.worker;
      workerToRemoveCacheIdx = findWorkerResult.indexInCache;

      if (hasNoJobs()){
        // if LoadBalancer has no job remove worker and move on
        CACHE.remove(workerToRemove);
        Set<Worker> removeWorkerResults = new LinkedHashSet<>();
        removeWorkerResults.add(workerToRemove);
        return removeWorkerResults;
      }
      else if (previousWorkerIdx >= workerToRemoveCacheIdx){
        CACHE.remove(workerToRemove);
        decrementPreviousWorkerIdx();
        System.out.println("\t\t[LoadBalancer#removeWorker()] decrement");
        // since `workerToRemove` is removed from CACHE we can call addJob() to redistribute the worker's jobs
        return addJob(workerToRemove.getJobSet());
      }else{
        CACHE.remove(workerToRemove);
        System.out.println("\t\t[LoadBalancer#removeWorker()] no decrement");
        // since `workerToRemove` is removed from CACHE we can call addJob() to redistribute the worker's jobs
        return addJob(workerToRemove.getJobSet());

      }
    }else{
      return null;
    }
  }

  /**
   * Add a job to a worker managed by LoadBalancer
   *
   * @param jobName name that identifies a job
   */
  public Worker addJob(String jobName){
    if (jobName.isEmpty())
      return null;

    // FixMe: Should be an exception but for now just add to log
    if (CACHE.isEmpty()){
      System.out.println("Trying to add job but LoadBalancer has no workers. Contradiction!");
      return null;
    }


    //System.out.println();();("[LoadBalancer#add(jobName)] Adding job "+jobName);
    Worker worker;

    // update the global jobSet
    allJobSet.add(jobName);

    // add round-robin style
    if (previousWorkerIdx == null) {
      worker = CACHE.get(0);
      worker.addJob(jobName);
      previousWorkerIdx = 0;

    } else {
      int nextWorkerIdx = (previousWorkerIdx + 1) % CACHE.size();
      worker = CACHE.get(nextWorkerIdx);
      worker.addJob(jobName);
      incrementPreviousWorkerIdx();
    }

    return worker;
  }

  /**
   * Add a collection of jobs to worker's managed by LoadBalancer
   *
   * @param jobNames the collection of job names to be added to the LoadBalancer
   * @return An array set of {@link Worker}
   */
  public Set<Worker> addJob(Collection<String> jobNames) {
    if (jobNames.isEmpty())
      return null;


    Set<Worker> workers = new LinkedHashSet<>();
      jobNames.forEach((jobName) -> workers.add(addJob(jobName)));
      return workers;

  }

  /**
   * Removes a job from a worker managed by LoadBalancer <br>
   * There are 4 cases:
   * <ul>
   *   <li><i>Case 1: A switch is needed isSwitchNeeded() == true</i> A job was removed and it would
   *       be optimal to switch the ordering of the workers in the CACHE rather than rebalance the
   *       jobs within the workers
   *   <li><i>Case 2:Re-balancing, isRebalanceNeeded() == true</i> A job was removed and it is
   *       required to take a job from the worker identified by `previousWorkerIdx` to the worker
   *       with the deleted job
   *   <li><i>Case 3: No rebalance is needed </i> Hence `previousWorkerIdx` is the the worker with
   *       the deleted job, so deleted the job and move on nothing special
   *   <li><i>Case 4: job is not managed by the LoadBalancer i </i> do nothing
   * </ul>
   *
   * @param jobName the job name to be removed
   * @return
   *     <ul>
   *       <li>singleton set containing Worker that contained deleted job if no rebalancing
   *       <li>a tuple set containing the Worker that contained the deleted job and the Worker of
   *           the rebalanced worker instance if there is rebalancing
   *       <li>null if the job name could not be found
   *     </ul>
   */
  public Set<Worker> removeJob(String jobName) {
    if (jobName.isEmpty()||hasNoJobs())
      return null;



    Set<Worker> removedJobResult = null;
    if (!(previousWorkerIdx == null)) {
      Worker workerPrev = CACHE.get(previousWorkerIdx);

      int indexWithJobName;
      if ((indexWithJobName = findIndexWithJobName(jobName)) != -1) {

        // update the global jobSet
        allJobSet.remove(jobName);

        // get Worker that contains the job to be deleted and add it to `removedJobResult`
        Worker workerWithDeletedJob = CACHE.get(indexWithJobName);
        removedJobResult = new LinkedHashSet<>();
        removedJobResult.add(workerWithDeletedJob);

        if (isSwitchNeeded(workerPrev, workerWithDeletedJob)) {
          System.out.println("\t\t[LoadBalancer#removeJob(String)] Switched order deletion for "+jobName);

          // remove the job
          workerWithDeletedJob.removeJob(jobName);

          // perform the switch
          CACHE.set(previousWorkerIdx, workerWithDeletedJob);
          CACHE.set(indexWithJobName, workerPrev);
        } else if (isRebalanceNeeded(workerPrev, workerWithDeletedJob)) {
          System.out.println("\t\t[LoadBalancer#removeJob(String)] Rebalanced deletion for "+jobName);

          // remove the job
          workerWithDeletedJob.removeJob(jobName);

          // perform the re-balance
          String rebalancedJob = workerPrev.getElementFromJobSet();
          workerPrev.removeJob(rebalancedJob);
          workerWithDeletedJob.addJob(rebalancedJob);

          // add the rebalanced worker to the `removedJobResult'
          removedJobResult.add(workerPrev);
        } else {
          System.out.println("\t\t[LoadBalancer#removeJob(String)] Regular deletion for "+jobName);
          // remove the job
          workerWithDeletedJob.removeJob(jobName);
        }
        decrementPreviousWorkerIdx();
      }
    }else{
      System.out.println("\t\t[LoadBalancer#removeJob(String)] job "+jobName+" not found");
    }

    return removedJobResult;
  }

  /**
   * Delete a collection of jobs managed by LoadBalancer
   *
   * @param jobNames a collection of jobs to be deleted
   * @return An set of sets of Workers that have been modified
   */
  public Set<Set<Worker>> removeJob(Collection<String> jobNames) {
    if (jobNames.isEmpty())
      return null;

    Set<Set<Worker>> removedJobResult = new LinkedHashSet<>();

    jobNames.forEach((jobName) -> removedJobResult.add(removeJob(jobName)));
    return removedJobResult;
  }

  /** Overrides the toString() method to show the LoadBalancer.CACHE instead of the LoadBalancer object */
  @Override
  public String toString() {
    return CACHE.toString();
  }

  /* *********************************************************************************
   *                               Custom Data Classes
   * ***********************************************************************************/

  /**
   * A <b>workerID</b> is the unique identifier for a kafka job worker thread instance <br>
   * A <b>jobSet</b> is a set of jobs <br>
   * A <b>Worker</b> is a class that encapsulates the relation between a workerID and an its associated
   * jobSet
   */
  class Worker {
    private String workerID;
    private LinkedHashSet<String> jobSet;

    Worker(String workerID) {
      this.workerID = workerID;
      this.jobSet = new LinkedHashSet<>();
    }

    public LinkedHashSet<String> getJobSet() {
      return jobSet;
    }

    public String getWorkerID() {
      return workerID;
    }

    boolean hasJob(String jobName) {
      return jobSet.contains(jobName);
    }

    void removeJob(String jobName) {
      jobSet.remove(jobName);
    }

    void addJob(String jobName) {
      jobSet.add(jobName);
    }

    Integer getNumberOfJobs() {
      return jobSet.size();
    }

    /**
     * Returns random jobName from jobSet or "" if jobSet is empty
     *
     * @return jobName
     */
    String getElementFromJobSet() {
      for (String jobName : jobSet) {
        return jobName;
      }
      return null;
    }

    @Override
    public String toString() {
      return "{workerID: " + workerID + ", jobSet:" + jobSet + "}";
    }

    @Override
    public int hashCode() {
      return workerID.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return ((obj instanceof Worker) && (((Worker) obj).workerID.equals(workerID)))
          | workerID.equals(obj);
    }
  }

  public class NoWorkerException extends Exception {
    public NoWorkerException(String errorMessage) {
      super(errorMessage);
    }
  }

  /* *********************************************************************************
   *                             Private Functions
   * ***********************************************************************************/


  /** Increments `previousWorkerIdx` by using the mod function */
  private void incrementPreviousWorkerIdx() {
    previousWorkerIdx = (previousWorkerIdx + 1) % CACHE.size();
  }

  /** Decrements `previousWorkerIdx` and wraps around once it goes negative */
  private void decrementPreviousWorkerIdx() {
    previousWorkerIdx = previousWorkerIdx - 1 < 0 ? CACHE.size() - 1 : previousWorkerIdx - 1;
  }

  /**
   * Searches the `CACHE' for the worker that holds the job name
   *
   * @param jobName the jobName searched for
   * @return index of the worker that holds the job name or -1 if not found.
   */
  private Integer findIndexWithJobName(String jobName) {
    int index = 0;
    for (Worker worker : CACHE) {
      if (worker.hasJob(jobName)) {
        return index;
      }
      index++;
    }
    return -1;
  }

  /**
   * Returns the Worker given a worker ID
   * @param workerID workerID of the worker to find
   */
  private FindWorkerResult findWorker(String workerID){
    int i=0;
    FindWorkerResult findWorkerResult = new FindWorkerResult();
    for (Worker worker : CACHE) {
      if (worker.getWorkerID().equals(workerID)) {
        findWorkerResult.indexInCache = i;
        findWorkerResult.worker = worker;
        return findWorkerResult;
      }
      i++;
    }
    return null;
  }

  /**
   * A class to hold the CACHE index and the Worker of a found worker.
   */
  private class FindWorkerResult{
    int indexInCache;
    Worker worker;
  }



  /**
   * Checks if a ordering switch is necessary instead of a rebalance
   *
   * @param workerPrev Worker that previousWorkerIdx is pointing to
   * @param workerWithDeletedJob Worker that contains the job to be deleted
   * @return a boolean that equals true if switch is necessary
   */
  private boolean isSwitchNeeded(Worker workerPrev, Worker workerWithDeletedJob) {
    return workerWithDeletedJob.getNumberOfJobs().equals(workerPrev.getNumberOfJobs());
  }

  /**
   * Checks if a rebalance is necessary
   *
   * @param workerPrev Worker that previousWorkerIdx is pointing to
   * @param workerWithDeletedJob Worker that contains the job to be deleted
   * @return a boolean that equals true if rebalance is necessary
   */
  private boolean isRebalanceNeeded(Worker workerPrev, Worker workerWithDeletedJob) {
    return workerWithDeletedJob.getNumberOfJobs() < workerPrev.getNumberOfJobs();
  }
}
