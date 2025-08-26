import asyncio
import pytest
from fastapi import WebSocketDisconnect
from uuid import uuid4
from unittest.mock import AsyncMock
from src.master_node.models.payloads import (
    JobRequestPayload,
    JobResponsePayload,
    MethodEnum,
    WebsocketMessage,
    MessageType
)
from src.master_node.services.master_node_websocket_server_service import (
    InvalidWorkerResponseError,
    MasterNodeWebsocketServerService,
    RPCRequestTimeoutError,
    WorkerCommunicationError,
    WorkerNotConnectedError
)

class TestMasterNodeWebsocketServerService:
    """Unit tests for MasterNodeWebsocketServerService"""
    
    @pytest.fixture
    def service(self):
        return MasterNodeWebsocketServerService()
    
    @pytest.fixture
    def mock_websocket(self):
        """Create a mock websocket"""
        websocket = AsyncMock()
        websocket.accept = AsyncMock()
        websocket.send_json = AsyncMock()
        websocket.receive_json = AsyncMock()
        return websocket

    @pytest.fixture
    def sample_job_request(self):
        return JobRequestPayload(
            request_id=uuid4(),
            method=MethodEnum.RUN,
            headers={"Content-Type": "application/json"},
            params={"param1": "value1"},
            body={"task": "test_task", "data": "test_data"}
        )

    @pytest.fixture
    def sample_job_response(self, sample_job_request: JobRequestPayload):
        """Create a sample job response payload"""
        return JobResponsePayload(
            request_id=sample_job_request.request_id,
            status = "ok",
            result = {"output": "test_result"},
            error = None,
            meta = {"execution_time": 1.5}
        )
    
    @pytest.mark.asyncio
    async def test_connect_accepts_and_cleans_connection(
        self,
        service: MasterNodeWebsocketServerService,
        mock_websocket: AsyncMock
    ):  
        worker_id = uuid4()
        mock_websocket.receive_json.side_effect = [
            {"type": "heartbeat", "payloads": {}},
            WebSocketDisconnect()
        ]

        await service.connect(worker_id, mock_websocket)

        assert str(worker_id) not in service.connections
        mock_websocket.accept.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_send_job_rpc_to_worker_node_success(
        self,
        service: MasterNodeWebsocketServerService,
        mock_websocket: AsyncMock, 
        sample_job_request: JobRequestPayload, 
        sample_job_response: JobResponsePayload
    ):
        """Test successful job request sending and response handling"""
        worker_id = uuid4()
        service.connections[str(worker_id)] = mock_websocket
        
        async def simulate_response():
            await asyncio.sleep(0.1)
            request_id = str(sample_job_request.request_id)
            if request_id in service.pending_requests:
                response = sample_job_response.model_copy(update={"request_id": sample_job_request.request_id})
                future = service.pending_requests[request_id]
                future.set_result(response.model_dump())
        
        response_task = asyncio.create_task(simulate_response())
        
        try:
            # Make the RPC call
            result = await service.send_job_rpc_to_worker_node(
                worker_id=worker_id,
                job_payload=sample_job_request,
                timeout=1.0
            )
            
            assert isinstance(result, JobResponsePayload)
            assert result.request_id == sample_job_request.request_id
            assert result.status == "ok"
            mock_websocket.send_json.assert_called_once()
        finally:
            if not response_task.done():
                response_task.cancel()
    
    @pytest.mark.asyncio
    async def test_send_job_rpc_to_worker_node_worker_not_connected(
        self,
        service: MasterNodeWebsocketServerService,
        sample_job_request: JobRequestPayload
    ):
        """Test job request fails when worker is not connected"""
        worker_id = uuid4()
        
        with pytest.raises(WorkerNotConnectedError):
            await service.send_job_rpc_to_worker_node(
                worker_id=worker_id,
                job_payload=sample_job_request
            )
    
    @pytest.mark.asyncio
    async def test_send_job_rpc_to_worker_node_timeout(
        self, 
        service: MasterNodeWebsocketServerService, 
        sample_job_request: JobRequestPayload, 
        mock_websocket: AsyncMock
    ):
        """Test job request timeout handling"""
        
        worker_id = uuid4()
        service.connections[str(worker_id)] = mock_websocket

        with pytest.raises(RPCRequestTimeoutError):
            await service.send_job_rpc_to_worker_node(
                worker_id,
                sample_job_request,
                timeout=1.0
            )
            
        assert len(service.pending_requests) == 0
    
    @pytest.mark.asyncio
    async def test_send_job_rpc_to_worker_node_invalid_response(
        self, 
        service: MasterNodeWebsocketServerService, 
        sample_job_request: JobRequestPayload, 
        mock_websocket: AsyncMock
    ):
        """Test job request with invalid response format"""
        
        worker_id = uuid4()
        service.connections[str(worker_id)] = mock_websocket
        
        async def simulate_invalid_response():
            await asyncio.sleep(0.1)
            request_id = str(sample_job_request.request_id)
            if request_id in service.pending_requests:
                future = service.pending_requests[request_id]
                future.set_result({"invalid": "response_format"})  # Missing required fields
        
        response_task = asyncio.create_task(simulate_invalid_response())
        
        try:
            with pytest.raises(InvalidWorkerResponseError):
                await service.send_job_rpc_to_worker_node(
                    worker_id=worker_id,
                    job_payload=sample_job_request,
                    timeout=1.0
                )
        finally:
            if not response_task.done():
                response_task.cancel()
                await response_task
    
    @pytest.mark.asyncio
    async def test_send_websocket_message_success(
        self, 
        service: MasterNodeWebsocketServerService,
        mock_websocket: AsyncMock,
    ):
        """Test successful WebSocket message sending"""
        
        worker_id = uuid4()
        service.connections[str(worker_id)] = mock_websocket
        
        ws_message = WebsocketMessage(
            type=MessageType.HEARTBEAT,
            request_id=None,
            payloads={"test": "data"}
        )
        
        await service.send_websocket_message_to_worker(worker_id, ws_message)
        
        mock_websocket.send_json.assert_called_once()
        call_args = mock_websocket.send_json.call_args[0][0]
        assert call_args["type"] == MessageType.HEARTBEAT
    
    @pytest.mark.asyncio
    async def test_send_to_worker_fire_and_forget(
        self, 
        service: MasterNodeWebsocketServerService,
        mock_websocket: AsyncMock,
    ):
        worker_id = uuid4()
        service.connections[str(worker_id)] = mock_websocket
        
        ws_message = WebsocketMessage(
            type=MessageType.HEARTBEAT,
            request_id=None,
            payloads={"test": "data"}
        )
        
        await service.send_to_worker(worker_id, ws_message)
        
        mock_websocket.send_json.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_websocket_message_worker_not_connected(
        self,
        service: MasterNodeWebsocketServerService
    ):
        """Test sending message when worker not connected"""
        
        worker_id=uuid4()
        ws_message = WebsocketMessage(
            type=MessageType.HEARTBEAT,
            request_id=None,
            payloads={"test": "data"}
        )
        
        with pytest.raises(WorkerNotConnectedError):
            await service.send_websocket_message_to_worker(worker_id, ws_message)
    
    @pytest.mark.asyncio
    async def test_send_websocket_message_communication_error(
        self,
        service: MasterNodeWebsocketServerService,
        mock_websocket: AsyncMock
    ):
        """Test communication error during message sending"""
        
        worker_id = uuid4()
        service.connections[str(worker_id)] = mock_websocket
        mock_websocket.send_json.side_effect = Exception("Connection lost")
        
        ws_message = WebsocketMessage(
            type=MessageType.HEARTBEAT,
            request_id=None,
            payloads={"test": "data"}
        )
        
        with pytest.raises(WorkerCommunicationError):
            await service.send_websocket_message_to_worker(worker_id, ws_message)
        
        # Verify worker was removed from connections
        assert not service.is_worker_connected(worker_id)
    
    def test_get_connected_workers(
        self,
        service: MasterNodeWebsocketServerService,
        mock_websocket: AsyncMock
    ):
        """Test getting list of connected workers"""
        
        worker_id1 = uuid4()
        worker_id2 = uuid4()
        service.connections[str(worker_id1)] = mock_websocket
        service.connections[str(worker_id2)] = mock_websocket

        connected_workers = service.get_connected_workers()

        assert len(connected_workers) == 2
        assert str(worker_id1) in connected_workers
        assert str(worker_id2) in connected_workers
    
    def test_is_worker_connected(
        self,
        service: MasterNodeWebsocketServerService,
        mock_websocket: AsyncMock
    ):
        """Test checking if a worker is connected"""
        
        worker_id = uuid4()
        
        assert not service.is_worker_connected(worker_id)
        
        service.connections[str(worker_id)] = mock_websocket
        
        assert service.is_worker_connected(worker_id)

    def test_get_pending_requests_count(
        self,
        service: MasterNodeWebsocketServerService,
    ):
        """Test getting count of pending requests"""

        assert service.get_pending_requests_count() == 0
        
        service.pending_requests["req1"] = asyncio.Future()
        service.pending_requests["req2"] = asyncio.Future()
        
        assert service.get_pending_requests_count() == 2

    @pytest.mark.asyncio
    async def test_full_rpc_flow(
        self,
        service: MasterNodeWebsocketServerService,
        sample_job_request: JobRequestPayload,
    ):
        """Test complete RPC flow with mock worker"""
        
        worker_id=uuid4()
                
        mock_websocket = AsyncMock()
        service.connections[str(worker_id)] = mock_websocket
        assert service.is_worker_connected(worker_id)
        
        expected_response = JobResponsePayload(
            request_id=sample_job_request.request_id,
            status="ok",
            error=None,
            result={"output": "success"}
        )
        
        rpc_task = asyncio.create_task(
            service.send_job_rpc_to_worker_node(
                worker_id=worker_id,
                job_payload=sample_job_request,
                timeout=10.0
            )
        )
        
        await asyncio.sleep(0.01)
    
        # Now simulate the worker response
        request_id = str(sample_job_request.request_id)
        if request_id in service.pending_requests:
            future = service.pending_requests[request_id]
            if not future.done():
                future.set_result(expected_response.model_dump())

        result = await rpc_task
            
        # Verify result
        assert result.request_id == sample_job_request.request_id
        assert result.status == "ok"
        assert result.result == {"output": "success"}
        mock_websocket.send_json.assert_called_once()
        assert service.get_pending_requests_count() == 0
