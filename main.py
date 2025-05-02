import json
import logging
import time
import uuid

from pycrescolib.clientlib import clientlib
from stunnel_tools import STunnelTools

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


# Connection parameters
host = 'localhost'
port = 8282
service_key = 'a6f7f889-2500-46d3-9484-5b6499186456'

# Connect to Cresco
client = clientlib(host, port, service_key)

if client.connect():

    try:
        logger.info(f"Connected to Cresco at {host}:{port}")

        # Get global region and agent from the connection, otherwise you can specify
        global_region = client.api.get_global_region()
        global_agent = client.api.get_global_agent()
        logger.info(f"Global region: {global_region}, Global agent: {global_agent}")

        # Create Stunnel Tools object
        stunnel_tools = STunnelTools(client, logger)

        # To reference the tunnel during operational, we will provide it a identifier on creation.
        # This identifier will be used for both the pipeline_id and the stunnel_id as there is a 1:1 relationship between
        # the pairs of plugins deployed in the pipeline and the stunnel src->dst created by the config.
        stunnel_id = str(uuid.uuid1())

        # src_region and src_agent is the agent where the new socket will be created
        src_region = global_region
        src_agent = global_agent
        # src_port this the port number of the new socket that will be created and tunneled to an existing socket
        src_port = '5202'

        # dst_region and dst_agent is the agent that will contact the existing socket.
        # It does not have to be on the same server as the socket, but the host and socket must be reachable
        dst_region = global_region
        dst_agent = global_agent
        # dst_host is the ip or hostname of the server with the remote service
        dst_host = 'localhost'
        # dst_port the port of the remote server we want to tunnel from
        dst_port = '5201'
        # buffer_size is the size of buffer used to communicate payloads on the Cresco dataplane.
        # The larger, the buffer, the more memory used but the higher the throughput.
        # The default message size of ActiveMQ is 100KiB, the average payload size on the public internet is between 20 and 1,500 bytes
        buffer_size = '8192'

        # This function deploys the stunnel pipeline, a pair of stunnel plugins, to the source and destination agent
        # The pipeline configuration, including the identifiers of the stunnel plugins is returned if the deployment was successful
        pipeline_config = stunnel_tools.deploy_stunnel_pipeline(stunnel_id, src_region, src_agent, dst_region, dst_agent)

        if pipeline_config is not None:
            # This function pushes the stunnel configuration to the src plugin, which in turn configures the destination
            # The results of the pipeline configuration are turned
            deployment_result = stunnel_tools.deploy_stunnel_config(pipeline_config, src_port, dst_host, dst_port, buffer_size)

            if deployment_result['status'] == '10':
                logger.info("Stunnel config deployment completed successfully")
                logger.info("Enabling Stunnel Performance Logger... This may take a few seconds...")
                dp = stunnel_tools.enable_performance_logger(stunnel_id)

                # Connect dataplane
                if dp.connect():
                    logger.info(f"Successfully connected to dataplane stream")

                    user_input = input("Press Enter to exit... \n")

                    logger.info("Removing Stunnel pipeline deployment")
                    stunnel_tools.remove_stunnel_pipeline(stunnel_id)

            else:
                logger.error("Stunnel config deployment failed")

        else:
            logger.error("Stunnel pipeline deployment failed")


    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        # Always close the client when done
        logger.info("Closing Cresco connection")
        client.close()
else:
    logger.error("Failed to connect to Cresco server")