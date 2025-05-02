import json
import os
import time
import logging
from typing import Dict, Any
from urllib import request

from pycrescolib.utils import decompress_param, get_jar_info


class STunnelTools:

    def __init__(self, client, logger=None):
        """
        Initialize the DataplaneTest class with a Cresco client

        Args:
            client: A connected pycrescolib clientlib instance
            logger: Optional logger instance (will create one if not provided)
        """
        # Store the client reference
        self.client = client

        # Setup logging if not provided
        if logger:
            self.logger = logger
        else:
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger(__name__)

    def text_callback(self, message):
        """Callback for handling text messages from the dataplane"""
        try:
            # Try to parse as JSON for better formatting
            try:
                json_msg = json.loads(message)
                self.logger.info(f"Text message (JSON): {json.dumps(json_msg, indent=2)}")
            except json.JSONDecodeError:
                # Not JSON, log as plain text
                self.logger.info(f"Text message: {message}")
        except Exception as e:
            self.logger.error(f"Error in text callback: {e}")

    def binary_callback(self, data):
        """Callback for handling binary messages from the dataplane"""
        try:
            self.logger.info(f"Binary data received: {len(data)} bytes")

            # You could process the binary data according to your needs
            # For example, if it's an image, save it:
            # with open("received_image.jpg", "wb") as f:
            #     f.write(data)

            # Or if it's UTF-8 text in binary form, you could decode it:
            try:
                text = data.decode('utf-8')
                self.logger.info(f"Binary data decoded as UTF-8: {text[:100]}...")
            except UnicodeDecodeError:
                self.logger.info("Binary data is not valid UTF-8")

        except Exception as e:
            self.logger.error(f"Error in binary callback: {e}")

    def enable_performance_logger(self, stunnel_id):

        stream_name = "stunnel_id='" + stunnel_id + "' AND type is NOT NULL";

        # Create dataplane with callbacks
        dp = self.client.get_dataplane(
            stream_name,
            self.text_callback,
            self.binary_callback
        )

        return dp

    def remove_stunnel_pipeline(self, stunnel_id) -> None:
        """Remove a file repository system across multiple nodes."""

        pipeline_list = self.client.globalcontroller.get_pipeline_list()
        for pipeline in pipeline_list:
            if pipeline['pipeline_name'] == stunnel_id:
                self.logger.info(f"Removing stunnel pipeline: {pipeline}")
                pipeline_id = pipeline['pipeline_id']
                self.client.globalcontroller.remove_pipeline(pipeline_id)

    def deploy_stunnel_pipeline(self, stunnel_id, src_region, src_agent, dst_region, dst_agent) -> None:
        """Deploy a file repository system across multiple nodes.

        Args:
            client: The Cresco client
            dst_region: Target region (controller)
            dst_agent: Target agent (controller)
        """
        self.logger.info(f"Starting multi-node stunnel deployment with controller at {dst_region}/{dst_agent}")

        try:

            # (1) Make sure the stunnel plugin exists on the GC, if not deploy it
            # If a URL is used we will always download the file
            plugin_source = "https://github.com/CrescoEdge/stunnel/releases/download/1.2-SNAPSHOT/stunnel-1.2-SNAPSHOT.jar"
            configparams = self.repo_plugin_check(plugin_source)

            # (2) Deploy the plugins to the agent(s) where you want to enable tunnels
            # * Note this just pushes the plugins to the agent(s) it does not establish stunnel configurations

            cadl = {
                'pipeline_id': '0',
                'pipeline_name': stunnel_id,
                'nodes': [],
                'edges': []
            }

            params0 = {
                'pluginname': configparams['pluginname'],
                'md5': configparams['md5'],
                'version': configparams['version'],
                'location_region': src_region,
                'location_agent': src_agent,
                #'src_port': src_port,
                #'stunnel_id': stunnel_id,
                #'buffer_size': buffer_size,
            }

            node0 = {
                'type': 'dummy',
                'node_name': 'SRC Plugin',
                'node_id': 0,
                'isSource': False,
                'workloadUtil': 0,
                'params': params0
            }

            # Destination node (controller)
            params1 = {
                'pluginname': configparams['pluginname'],
                'md5': configparams['md5'],
                'version': configparams['version'],
                'location_region': dst_region,
                'location_agent': dst_agent,
            }

            node1 = {
                'type': 'dummy',
                'node_name': 'DST Plugin',
                'node_id': 1,
                'isSource': False,
                'workloadUtil': 0,
                'params': params1
            }

            # Edge
            edge0 = {
                'edge_id': 0,
                'node_from': 0,
                'node_to': 1,
                'params': {}
            }

            cadl['nodes'].append(node0)
            cadl['nodes'].append(node1)
            cadl['edges'].append(edge0)

            # Submit pipeline
            reply = self.client.globalcontroller.submit_pipeline(cadl)
            pipeline_id = reply['gpipeline_id']

            # this is needed for the config
            pipeline_config = self.client.globalcontroller.get_pipeline_info(pipeline_id)

            self.logger.info(f"Pipeline Config: {pipeline_config}")

            # Wait for pipeline to come online
            is_online = self.wait_for_pipeline(pipeline_id)
            if is_online:
                # Note: Pipeline is not removed in this pycrescolib_test to allow ongoing sync
                self.logger.info("Multi-node file repository pycrescolib_test completed successfully")
                # only return if pipeline deployment was successful
                return pipeline_config
            else:
                self.logger.info("Multi-node file repository pycrescolib_test failed")

        except Exception as e:
            self.logger.error(f"Error in filerepo_deploy_multi_node: {e}", exc_info=True)

    def deploy_stunnel_config(self, pipeline_config, src_port, dst_host, dst_port, buffer_size):


        try:

            stunnel_id = pipeline_config['pipeline_name']

            src_region = pipeline_config['nodes'][0]['params']['location_region']
            src_agent = pipeline_config['nodes'][0]['params']['location_agent']
            src_plugin = pipeline_config['nodes'][0]['node_id']

            dst_region = pipeline_config['nodes'][1]['params']['location_region']
            dst_agent = pipeline_config['nodes'][1]['params']['location_agent']
            dst_plugin = pipeline_config['nodes'][1]['node_id']

            message_event_type = 'CONFIG'
            message_payload = {
                'action': 'configsrctunnel',
                'action_src_port': src_port,
                'action_dst_host': dst_host,
                'action_dst_port': dst_port,
                'action_dst_region': dst_region,
                'action_dst_agent': dst_agent,
                'action_dst_plugin': dst_plugin,
                'action_buffer_size': buffer_size,
                'action_stunnel_id': stunnel_id,
            }

            deployment_result = self.client.messaging.global_plugin_msgevent(True, message_event_type, message_payload, src_region, src_agent, src_plugin)
            self.logger.info(f"Config result: {deployment_result}")
            return deployment_result

        except Exception as e:
            self.logger.error(f"Error in stunnel configuration deployment: {e}", exc_info=True)

    def get_plugin_from_git(self, src_url: str, force: bool = False) -> str:
        """Download plugin JAR file from GitHub.

        Args:
            src_url: URL to the plugin JAR
            force: Whether to force download even if file exists

        Returns:
            Local path to downloaded JAR file
        """
        folder_path = "plugins"
        # Check if the folder exists
        if not os.path.exists(folder_path):
            # Create the folder if it doesn't exist
            os.makedirs(folder_path)

        dst_file = src_url.rsplit('/', 1)[1]
        dst_path = os.path.join(folder_path, dst_file)


        if force or not os.path.exists(dst_path):
            self.logger.info(f"Downloading {dst_file} plugin from {src_url}")
            try:
                request.urlretrieve(src_url, dst_path)
                self.logger.info(f"Downloaded {dst_file} successfully")
            except Exception as e:
                self.logger.error(f"Failed to download {dst_file}: {e}")
                raise
        else:
            self.logger.info(f"Using existing plugin file: {dst_path}")

        return dst_path

    def upload_plugin(self, jar_path: str) -> Dict[str, Any]:
        """Upload a plugin to the global controller.

        Args:
            client: The Cresco client
            jar_path: Path to the JAR file

        Returns:
            Response from upload operation
        """
        self.logger.info(f"Uploading plugin {jar_path} to global controller")
        try:
            reply = self.client.globalcontroller.upload_plugin_global(jar_path)
            self.logger.info(f"Upload status: {reply.get('status_code', 'unknown')}")
            return reply
        except Exception as e:
            self.logger.error(f"Error uploading plugin: {e}")
            raise

    def repo_plugin_check(self, plugin_source: str) -> str:
        """Check if a plugin is present in the file repository."""

        try:

            # download if source is url
            if plugin_source.startswith('https://'):
                plugin_source = self.get_plugin_from_git(plugin_source)

            config_str = get_jar_info(plugin_source)

            plugin_name = config_str['pluginname']
            plugin_version = config_str['version']
            plugin_md5 = config_str['md5']

            upload_plugin = True

            message_event_type = 'EXEC'
            message_payload = {
                'action': 'listpluginsrepo',
            }

            reply = self.client.messaging.global_controller_msgevent(True, message_event_type, message_payload)
            reply = json.loads(decompress_param(reply['listpluginsrepo']))['plugins']
            for plugin in reply:
                if plugin['pluginname'] == plugin_name and plugin['version'] == plugin_version and plugin['md5'] == plugin_md5:
                    self.logger.info(f"Plugin {plugin_name} version {plugin_version} found in file repository")
                    upload_plugin = False
            self.logger.info(f"Plugin {plugin_name} version {plugin_version} not found in file repository")

            if upload_plugin:
                self.upload_plugin(plugin_source)

            # Get plugin configuration
            self.logger.info(f"Plugin config: {config_str}")

            return config_str

        except Exception as e:
            self.logger.error(f"Error checking plugin repository: {e}")

    def wait_for_pipeline(self, pipeline_id: str, target_status: int = 10, timeout: int = 60) -> bool:
        """Wait for pipeline to reach desired status.

        Args:
            client: The Cresco client
            pipeline_id: Pipeline ID to monitor
            target_status: Desired status code (default: 10 for online)
            timeout: Maximum wait time in seconds

        Returns:
            True if pipeline reached desired status, False otherwise
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                status = self.client.globalcontroller.get_pipeline_status(pipeline_id)
                if status == target_status:
                    self.logger.info(f"Pipeline {pipeline_id} reached status {target_status}")
                    return True

                self.logger.info(f"Waiting for pipeline {pipeline_id} to reach status {target_status}, current: {status}")
                time.sleep(2)
            except Exception as e:
                self.logger.error(f"Error checking pipeline status: {e}")
                time.sleep(2)

        self.logger.error(f"Timeout waiting for pipeline {pipeline_id} to reach status {target_status}")
        return False
