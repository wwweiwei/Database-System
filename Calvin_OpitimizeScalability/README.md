# Final Project - Phase 2 (Optimization)

Now we all have a basic implementation of Calvin. Let's make it better.

## Steps

1. Fork this project
2. Check if you can run Calvin with a benchmark properly.
3. Optimize Calvin
4. Run a few experiments to evaluate your optimization
5. Prepare slides to present your work
6. Share your work! (2020/6/29)
7. Write a report.
8. Commit, push your repo to Gitlab, and submit a merge request.

## Optimization Goal

In this project, there is no constraint on how you optimize the system. The only requirement is to make Calvin perform better under a metric that we assign:

Each group has one of the following goal assgined according to the group ids.

- For the group with id = 3N (N = any integer), please optimize total throughput with 3 servers (make it higher).
- For the group with id = 3N + 1 (N = any integer), please optimize latency with 3 servers (make it lower).
- For the group with id = 3N + 2 (N = any integer), please optimize scalability with the TPC-C benchmark.

In the final presentation, you have to use at least one experiment to show that your optimization improve the given goal.

## How to Run the Basic Calvin

1. Decide how many servers and clients to use
2. Setup addresses in `vanillacomm.properties`
3. Setup the number of partitions in `calvin.properties`
4. Setup benchmark settings in `vanillabench.properties`
5. Setup run configurations for each server and client
  - Server
    - project: `bench`
    - main class: `org.vanilladb.bench.server.StartUp`
    - program arguments: `[DB Name] [Server ID]` (e.g., `calvin-0 0`)
    - vm arguments:
      ```
      -Djava.util.logging.config.file=target/classes/java/util/logging/logging.properties
      -Dorg.vanilladb.bench.config.file=target/classes/org/vanilladb/bench/vanillabench.properties
      -Dorg.vanilladb.core.config.file=target/classes/org/vanilladb/core/vanilladb.properties
      -Dorg.vanilladb.comm.config.file=target/classes/org/vanilladb/comm/vanillacomm.properties
      -Dorg.vanilladb.calvin.config.file=target/classes/org/vanilladb/calvin/calvin.properties
      ```
  - Client
    - project: `bench`
    - main class: `org.vanilladb.bench.App`
    - program arguments: `[1(Load)/2(Bench)] [Client ID]` (e.g., `1 0`)
    - vm arguments:
      ```
      -Djava.util.logging.config.file=target/classes/java/util/logging/logging.properties
      -Dorg.vanilladb.bench.config.file=target/classes/org/vanilladb/bench/vanillabench.properties
      -Dorg.vanilladb.core.config.file=target/classes/org/vanilladb/core/vanilladb.properties
      -Dorg.vanilladb.comm.config.file=target/classes/org/vanilladb/comm/vanillacomm.properties
      -Dorg.vanilladb.calvin.config.file=target/classes/org/vanilladb/calvin/calvin.properties
      ```
6. Load your testbed
   1. Start all the servers
   2. Wait for all the server ready
   3. **Start a client**
   4. Wait for loading finished
   5. Close all programs
   6. Backup your database files
7. Benchmark
   1. Reset database files using the backup files
   2. Start all the servers
   3. Wait for all the server ready
   4. Start all the clinets
   5. Wait for benchmarking finished
   6. Check the result

## Final Presentation

The final presentation will be on 2020/6/29 (Mon.) in the class room. We will annonuce more about the exact start time later.

Each group has to present at least the following aspects:

- Your group id and team members
- Your optimization goal
- How do you optimize your system
- Your experiment results and analysis

## Submission

The procedure of submission is as following:

1. Fork our [final-project-opt](https://shwu10.cs.nthu.edu.tw/courses/databases/2020-spring/db20-final-project-opt) on GitLab
2. Clone the repository you forked
3. Finish your work and prepare for the presentation
4. Commit your work, push your work to GitLab.
   - Name your report as `[Team Member 1 ID]_[Team Member 2 ID]_final_project_opt_report`
     - E.g. `102062563_103062528_final_project_opt_report.pdf`
5. Open a merge request to the original repository.
   - Source branch: Your working branch.
   - Target branch: The branch with your team number. (e.g. `team-1`)
   - Title: `Team-X Submission` (e.g. `Team-1 Submission`).

## No Plagiarism Will Be Tolerated

If we find you copy someone’s code, you will get 0 point for this assignment.
