# import os
# import hydra


# class RouteSampler(object):


#     def __call__(self, ):
#         # inherit the routeSampler arguments from the config file


# class CreateRouteSamplerInput:

#     """
#     This class creates the input files for the routeSampler.
#     It takes a dataframe of volumes. The dataframe is indexed by the time of the volume measurement.
#     The dataframe has columns for the volume of relationship.
#     """

#     def __init__(self,
#         volumes_df,
#         relationship_file,
#     ):


#     # def __call__(self, data):
#     #     # Path: pipelines/blocks/routesampler/routesampler.py
#     #     routeSampler.main(routeSampler.get_options([
#     #                 "-r",
#     #                 random_route_file,
#     #                 "-t",
#     #                 turn_file_path,
#     #                 "-o",
#     #                 output_file_path,
#     #                 "--optimize",
#     #                 "full",
#     #                 *output_type,
#     #                 # "-V",  # I don't need it to be verbose
#     #                 "-a",
#     #                 trip_attribute_string,
#     #                 "--threads",
#     #                 str(threads),
#     #                 "--seed",
#     #                 str(random_seed),
#     #                 "--prefix",
#     #                 vehicle_prefix,
#     #                 # "-i",
#     #                 # "1200",
#     #                 "--weighted",
#     #                 "--minimize-vehicles",
#     #                 str(random.uniform(.6, 1) if minimize_vehicles_rand else 0.8),
#     #                 #  "--min-count",
#     #                 #  "1"
#     #             ]))
