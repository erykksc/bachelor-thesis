# Notes to the book "Cloud Service Benchmarking" by David Bermbach et al.
The exact book name is "Cloud Service Benchmarking : Measuring Quality of Cloud
Services From a Client Perspective" by David Bermbach, Erik Wittern, Stefan Tai

## Main questions and goals of the book
What is cloud service benchmarking and why should I care about it?

What are critical objectives and components I need to consider while designing a benchmark?

With which challenges do I have to deal when implementing and running an actual benchmark?

What and how can I learn from my measurement results?


## Ideas for the thesis
Benchmark elastic scalability (what if during stress testing or some workload
I decide to add another worker node, will it slow down/speed up the system).
Multiple tests over times as the quality of cloud provider may change.

There are database-centric benchmarking standards set by 
Transaction Processing Performance Council (TPC) 
and system performance benchmarks defined by Standard Performance Evaluation 
Corporation (SPEC)


### What is cloud service?
a software system running in the cloud whose functionality is consumed
programmatically by applications over Internet protocols.

### What is quality of cloud service?
Examples of qualities: availability, latency, scalability

### What is cloud service benchmarking
Cloud service benchmarking is a way to systematically study the quality
of cloud services based on experiments.

### Benchmarking vs monitoring
Monitoring tries to be non intrusive while benchmarking puts load on the system,
but sometimes the line is blurred ex. Netflix injects failures to detect fault 
tolerances
Benchmarking contains load generator


## Building blocks of benchmarks
### Design
