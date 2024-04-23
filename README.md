# sumo-pipelines

Allows for a end to end SUMO pipeline definition using standalone processing **[blocks](https://github.com/mschrader15/sumo-pipelines/tree/main/sumo_pipelines/blocks)** configured using YAML. 


Supports massive parallelization using Ray, as well as parallel calibration using Ray Tune and Nevergrad.


## Used By:

### [sumo-cf-calibration](https://github.com/UnivOfAlabama-BittleResearchGroup/sumo-cf-calibration)

`sumo-pipelines` was used to calibrate car-following models using trajectory data from roadside radar.

Example Config:

```yaml
Metadata:
  # The name will also show up as the main folder for simulation
  author: mcschrader@crimson.ua.edu
  output: ${oc.env:PROJECT_ROOT}/tmp/${Metadata.name}/${datetime.now:%m.%d.%Y_%H.%M.%S}
  cwd: ${.output}/${.run_id}
  run_id: ???
  simulation_root: ${oc.env:PROJECT_ROOT}/sumo-xml
  random_seed: 7788

Blocks:
  TrajectoryGenerator:
    pair_file: "${oc.env:DATA_PATH}/leaders.parquet"
    max_queue_size: 64
    leader_id: ???
    follower_id: ???

  TrajectoryProcessing:
    generate_function: external.functions.trajectory_loaders.read_trajectories.database_loader
    kwargs:
      traj_file: "${oc.env:DATA_PATH}/processed_followers.parquet"
      follower_id: ${Blocks.TrajectoryGenerator.follower_id}
      leader_id: ${Blocks.TrajectoryGenerator.leader_id}
      step_length: ${Blocks.SimulationConfig.step_length}

  SimulationConfig:
    start_time: 0
    end_time: 10_000
    net_file: ${Metadata.simulation_root}/net.net.xml
    gui: True
    route_files:
      - ${Metadata.simulation_root}/route.rou.xml
    step_length: 1
    additional_files: null
    additional_sim_params:
      - --seed
      - ${Metadata.random_seed}
      - --start
      - "--step-method.ballistic"
    target_lane: E2_0
    route_name: r_0

  CFOptimizeConfig:
    optimization_algo: "NGOpt"
    budget: 2000
    simulation_config: ${Blocks.SimulationConfig}
    early_stopping: True
    early_stopping_tolerance: 100
    seed: ${Metadata.random_seed}

  Error:
    method: "spacing"
    error_func: "nrmse_s_v"
    val: "${.nrmse_s_v}"
    include_accel: True

Pipeline:
  executor: ray
  parallel_proc: auto
  pipeline:
    - block: CalibrationPipeline
      parallel: True
      number_of_workers: 64
      producers:
        - function: external.functions.sumo_pipelines_adapter.loader_adapter.trajectory_pair_generator
          config: ${Blocks.TrajectoryGenerator}
      consumers:
        - function: external.functions.sumo_pipelines_adapter.optimizer.optimize
          config: ${Blocks.CFOptimizeConfig}
      result_handler:
        function: external.functions.sumo_pipelines_adapter.optimizer.dump_results
        config: {}

```


### [sa-traffic-sim](https://github.com/UnivOfAlabama-BittleResearchGroup/sa-traffic-sim)

`sumo-pipelines` was used to perform comprehensive sensitivity analysis on the traffic simulation model.

```YAML
Metadata:
  # The name will also show up as the main folder for simulation
  name: SobolSensitivityAnalysisCars-${Blocks.SobolSequenceConfig.N}
  author: mcschrader@crimson.ua.edu
  output: ${oc.env:PROJECT_ROOT}/tmp/${Metadata.name}/${datetime.now:%m.%d.%Y_%H.%M.%S}
  cwd: ${.output}/${.run_id}
  run_id: ???
  simulation_root: ${oc.env:PROJECT_ROOT}/simulation
  random_seed: 42
Blocks:
  SobolSequenceConfig:
    N: 2048
    calc_second_order: False
    save_path: ${Metadata.output}/sobol_sequence.parquet
Pipeline:
  pipeline:
    - block: RunSimulation
      parallel: True
      number_of_workers: 64
      producers:
        - function: producers.generate_sobol_sequence
          config: ${Blocks.SobolSequenceConfig}
      consumers:
        - function: ${import:xml.update_output_file}
          config: ${Blocks.XMLChangeOutputConfig}
        - function: vehicle_distributions.create_simple_sampled_distribution
          config: ${Blocks.SampledSimpleCFConfig}
        - function: routesampler.call_random_trips
          config: ${Blocks.RandomTripsConfig}
        - function: routesampler.call_route_sampler
          config: ${Blocks.RouteSamplerConfig}
        - function: simulation.run_sumo
          config: ${Blocks.SimulationConfig}
        - function: ${import:xml.convert_xml_to_parquet}
          config: ${Blocks.XMLConvertConfig}
        - function: ${import:xml.convert_xml_to_parquet}
          config:
            source: ${Blocks.XMLChangeOutputConfig.changes[1].new_output}
            target: ${Metadata.cwd}/detectors.parquet
            delete_source: true
            elements:
              - name: interval
                attributes:
                  - begin
                  - end
                  - id
                  - sampledSeconds
                  - nVehEntered
                  - nVehLeft
                  - nVehSeen
                  - meanSpeed
                  - meanTimeLoss

        - function: external.src.sa_helpers.metrics.get_sa_results
          config:
            trip_info_file: ${Blocks.XMLConvertConfig.target}
            detector_file: ${Metadata.cwd}/detectors.parquet
            warmup_time: ${Blocks.SimulationConfig.warmup_time}
            total_fuel_l: ???
            average_fc: ???
            average_speed: ???
            average_delay: ???
            average_travel_time: ???
            delay_ratio: ???

        - function: emissions.fast_total_energy
          config: ${Blocks.FuelTotalConfig}

        - function: ${import:io.save_config}
          config:
            save_path: ${Metadata.cwd}/config.yaml

        - function: ${import:io.rm_file}
          config:
            rm_files:
              - ${Blocks.XMLChangeOutputConfig.changes[0].target}
              - ${Blocks.XMLChangeOutputConfig.changes[1].target}
              - ${Metadata.cwd}/detectors.parquet
              - ${Metadata.cwd}/random.trips.xml
              - ${Metadata.cwd}/vehDist.in.xml
              - ${Metadata.cwd}/routes.add.xml
              - ${Metadata.cwd}/routesampler.rou.xml
              - ${Metadata.cwd}/per_phase.e2.add.xml
```
