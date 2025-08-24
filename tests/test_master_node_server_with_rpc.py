import asyncio
from pydantic import ValidationError
import pytest
from asyncio import Future
from typing import Any
from fastapi import WebSocketDisconnect
from uuid import uuid4
from unittest.mock import AsyncMock, patch
from src.master_node.models.payloads import JobRequestPayload, JobResponsePayload, MethodEnum
from src.master_node.services.master_node_websocket_server_service import MasterNodeWebsocketServerService

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
        """Create a sample job request payload"""
        return JobRequestPayload(
            request_id=uuid4(),
            method=MethodEnum.RUN,
            headers={"Content-Type": "application/json"},
            params={"param1": "value1"},
            body={"task": "test_task", "data": "test_data"}
        )

    @pytest.fixture
    def sample_job_response(self):
        """Create a sample job response payload"""
        return JobResponsePayload(
            request_id=uuid4(),
            status = "ok",
            result = {"output": "test_result"},
            error = None,
            meta = {"execution_time": 1.5}
        )
    
    @pytest.mark.asyncio
    async def test_connect_stores_worker_connection(self, service: MasterNodeWebsocketServerService, mock_websocket: AsyncMock):
        """Test that connect method stores worker connection"""
        
        worker_id = (uuid4())
        mock_websocket.receive_json.side_effect = [{"type": "heartbeat"}, WebSocketDisconnect()]

        await service.connect(worker_id, mock_websocket)

        assert str(worker_id) not in service.connections
        mock_websocket.accept.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_connect_handles_websocket_disconnect(self, service: MasterNodeWebsocketServerService, mock_websocket: AsyncMock):
        """Test that connect method handles WebSocket disconnect gracefully"""
        
        worker_id = (uuid4())
        mock_websocket.receive_json.side_effect = [{"type": "heartbeat"}, WebSocketDisconnect()]
        
        with patch("builtins.print") as mock_print:
            await service.connect(worker_id, mock_websocket)

        mock_print.assert_any_call(f"Worker {worker_id} disconnected normally")
        assert str(worker_id) not in service.connections
    
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

        async def mock_wait_for(future: Future[Any], timeout): # type: ignore
            future.set_result(sample_job_response.model_dump())
            return sample_job_response.model_dump()

        with patch("asyncio.wait_for", side_effect=mock_wait_for), \
            patch("builtins.print"):
            
            result = await service.send_job_rpc_to_worker_node(worker_id, sample_job_request, timeout=30.0)

        mock_websocket.send_json.assert_called_once_with(sample_job_request.model_dump())
        
        assert isinstance(result, JobResponsePayload)
        assert result.status == "ok"
        assert len(service.pending_requests) == 0
    
    @pytest.mark.asyncio
    async def test_send_job_rpc_to_worker_node_worker_not_connected(
        self,
        service: MasterNodeWebsocketServerService,
        sample_job_request: JobRequestPayload
    ):
        """Test job request fails when worker is not connected"""
        
        worker_id = uuid4()
        
        with pytest.raises(RuntimeError) as exc_info:
            await service.send_job_rpc_to_worker_node(worker_id, sample_job_request)
        
        assert f"Worker {worker_id} not connected" in str(exc_info.value)
    
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

        with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError), \
            patch("builtins.print") as mock_print:
            
            with pytest.raises(asyncio.TimeoutError) as exc_info:
                await service.send_job_rpc_to_worker_node(worker_id, sample_job_request, timeout=1.0)

        mock_print.assert_any_call(f"Job request to worker {worker_id} timed out after 1.0s")
        assert f"Worker {worker_id} did not respond within 1.0 seconds" in str(exc_info.value)
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
        
        invalid_response = {"invalid": "response"}
        
        async def mock_wait_for(future: Future[Any], timeout): # type: ignore
            future.set_result(invalid_response)
            return invalid_response
        
        with patch("asyncio.wait_for", side_effect=mock_wait_for), \
            patch("builtins.print"):
            with pytest.raises(RuntimeError):
                await service.send_job_rpc_to_worker_node(worker_id, sample_job_request)
    
    @pytest.mark.asyncio
    async def test_handle_worker_message_invalid_rpc_response(self, service: MasterNodeWebsocketServerService):
        """Test handling of invalid RPC response format"""
        
        worker_id = uuid4()
        request_id = str(uuid4())
        invalid_response = {"request_id": request_id, "invalid": "format"}
        
        future: Future[Any] = Future()
        service.pending_requests[request_id] = future
        
        with patch("builtins.print") as mock_print:
            await service.handle_worker_message(worker_id, invalid_response)
        
        assert future.done()
        assert isinstance(future.exception(), ValidationError)
        
        calls = [str(call) for call in mock_print.call_args_list]
        assert any(
            f"Invalid response format for request {request_id}: " in c and "validation error for JobResponsePayload" in c
            for c in calls
        )
    
    @pytest.mark.asyncio
    async def test_handle_worker_message_unknown_type(self, service: MasterNodeWebsocketServerService, mock_websocket: AsyncMock):
        """Test handling of messages with unknown type"""
        
        worker_id = uuid4()
        service.connections[str(worker_id)] = mock_websocket
        message = {"type": "unknown_type", "data": "some_data"}
        
        with patch("builtins.print") as mock_print:
            await service.handle_worker_message(worker_id, message)
        
        mock_print.assert_any_call(f"Unknown message type from worker {worker_id}: {message}")
        mock_websocket.send_json.assert_called_with({"error": "Unknown message type"})
    
    @pytest.mark.asyncio
    async def test_send_to_worker_success(
        self,
        service: MasterNodeWebsocketServerService,
        mock_websocket: AsyncMock
    ):
        """Test successful fire-and-forget message sending"""
        
        worker_id = uuid4()
        data = {"message": "test_data"}
        service.connections[str(worker_id)] = mock_websocket

        with patch("builtins.print") as mock_print:
            await service.send_to_worker(worker_id, data)

        mock_websocket.send_json.assert_called_once_with(data)
        mock_print.assert_any_call(f"Sent to {worker_id}: {data}")

    @pytest.mark.asyncio
    async def test_send_to_worker_not_connected(
        self,
        service: MasterNodeWebsocketServerService
    ):
        """Test send_to_worker fails when worker is not connected"""
        worker_id = uuid4()
        data = {"message": "test_data"}

        with pytest.raises(RuntimeError) as exc_info:
            await service.send_to_worker(worker_id, data)

        assert f"Worker {worker_id} not connected" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_send_to_worker_send_fails(
        self,
        service: MasterNodeWebsocketServerService,
        mock_websocket: AsyncMock
    ):
        """Test send_to_worker handles send failures"""
        
        worker_id = uuid4()
        data = {"message": "test_data"}
        service.connections[str(worker_id)] = mock_websocket
        mock_websocket.send_json.side_effect = Exception("Send failed")

        with patch("builtins.print") as mock_print:
            with pytest.raises(RuntimeError) as exc_info:
                await service.send_to_worker(worker_id, data)

        mock_print.assert_any_call(f"Failed to send to worker {worker_id}: Send failed")
        assert f"Failed to send to worker {worker_id}: Send failed" in str(exc_info.value)

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