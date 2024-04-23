# sumo-pipelines

Allows for a end to end SUMO pipeline definition using standalone processing **[blocks](https://github.com/mschrader15/sumo-pipelines/tree/main/sumo_pipelines/blocks)**. Supports massive parallelization using Ray, as well as parallel calibration using Ray Tune and Nevergrad.


## Used By:

### [sumo-cf-calibration](https://github.com/UnivOfAlabama-BittleResearchGroup/sumo-cf-calibration)

`sumo-pipelines` was used to calibrate car-following models using trajectory data from roadside radar.

### [sa-traffic-sim](https://github.com/UnivOfAlabama-BittleResearchGroup/sa-traffic-sim)

`sumo-pipelines` was used to perform comprehensive sensitivity analysis on the traffic simulation model.
