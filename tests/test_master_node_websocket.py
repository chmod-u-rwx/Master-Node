import asyncio
import datetime
from datetime import datetime, timezone
import json
from typing import Any, List
from unittest.mock import AsyncMock, Mock, patch
from uuid import UUID, uuid4
import pytest
import websockets
from src.master_node.services.master_node_websocket_server_service import MasterNodeWebsocketServerService

class TestMasterNodeWebsocketServerService:
    """
    Test the Websocket service behavior directly without network calls
    """
    
    @pytest.fixture
    def ws_service(self):
        return MasterNodeWebsocketServerService()
    
    @pytest.fixture
    def mock_websocket(self):
        mock_ws = Mock()
        mock_ws.accept = AsyncMock()
        mock_ws.send_json = AsyncMock()
        mock_ws.receive_json = AsyncMock()
        return mock_ws

    @pytest.mark.asyncio
    async def test_connect_accepts_websocket_and_stores_connection(
        self,
        ws_service: MasterNodeWebsocketServerService,
        mock_websocket: Mock
    ):
        """
        Test that connecting a worker properly accepts Websocket and stores it
        """
        worker_id = UUID(str(uuid4()))
        
        from fastapi import WebSocketDisconnect
        mock_websocket.receive_json.side_effect = WebSocketDisconnect()
        await ws_service.connect(worker_id, mock_websocket)
        
        mock_websocket.accept.assert_called_once()
        assert str(worker_id) not in ws_service.connections
    
    @pytest.mark.asyncio
    async def test_connect_stores_connection_during_session(
        self,
        ws_service: MasterNodeWebsocketServerService,
        mock_websocket: Mock
    ):
        """
        Test that connection is stored while active and can receive messages
        """
        
        worker_id = UUID(str(uuid4()))
        
        from fastapi import WebSocketDisconnect
        now = datetime.now(timezone.utc)
        mock_websocket.receive_json.side_effect = [
            {"type": "heartbeat", "timestamp": str(now)},
            WebSocketDisconnect()
        ]
        await ws_service.connect(worker_id, mock_websocket)
        
        mock_websocket.accept.assert_called_once()
        assert mock_websocket.receive_json.call_count == 2
    
    @pytest.mark.asyncio
    async def test_handle_heartbeat_message_logs_correctly(
        self,
        ws_service: MasterNodeWebsocketServerService
    ):
        """
        Test that heartbeat messages are processed and logged
        """
        
        worker_id = UUID(str(uuid4()))
        now = datetime.now(timezone.utc)
        message: dict[str, Any] = {"type": "heartbeat", "timestamp": str(now)}
        
        with patch("builtins.print") as mock_print:
            await ws_service.handle_worker_message(worker_id, message)
            mock_print.assert_called_with(f"Heartbeat from worker {worker_id}")
    
    @pytest.mark.asyncio
    async def test_handle_status_update_message_processes_status(
        self,
        ws_service: MasterNodeWebsocketServerService
    ):
        """
        Test that status update messages extract and log status correctly
        """
        
        worker_id = UUID(str(uuid4()))
        status = "processing_task_456"
        message = {"type": "status_update", "status": status}
        
        with patch("builtins.print") as mock_print:
            await ws_service.handle_worker_message(worker_id, message)
            mock_print.assert_called_with(f"Status update from worker {worker_id}: {status}")
    
    @pytest.mark.asyncio
    async def test_handle_message_missing_type_sends_specific_error(
        self,
        ws_service: MasterNodeWebsocketServerService,
        mock_websocket: Mock,
    ):
        """
        Test that messages without 'type' field trigger correct error response
        """
        
        worker_id = UUID(str(uuid4()))
        ws_service.connections[str(worker_id)] = mock_websocket
        
        now = datetime.now(timezone.utc)
        message: dict[str, Any] = {"data": "some data without type", "timestamp": str(now)}
        
        with patch("builtins.print") as mock_print:
            await ws_service.handle_worker_message(worker_id, message)
            mock_print.assert_called_with(f"Missing 'type' in message from worker {worker_id}: {message}")
            mock_websocket.send_json.assert_called_once_with({"error": "Missing 'type' in message"})
    
    @pytest.mark.asyncio
    async def test_handle_unknown_message_type_sends_specific_error(
        self,
        ws_service: MasterNodeWebsocketServerService,
        mock_websocket: Mock,
    ):
        """
        Test that unknown message types trigger correct error response
        """
        
        worker_id = UUID(str(uuid4()))
        ws_service.connections[str(worker_id)] = mock_websocket
        
        message = {"type": "unknown_message_type", "data": "some data"}
        
        with patch("builtins.print") as mock_print:
            await ws_service.handle_worker_message(worker_id, message)
            mock_print.assert_called_with(f"Unknown message type from worker {worker_id}: {message}")
            
            mock_websocket.send_json.assert_called_once_with({"error": "Unknown message type"})
    
    @pytest.mark.asyncio
    async def test_send_to_connected_worker_success(
        self,
        ws_service: MasterNodeWebsocketServerService,
        mock_websocket: Mock,
    ):
        """
        Test that sending data to a connected worker works correctly
        """
        
        worker_id = UUID(str(uuid4()))
        ws_service.connections[str(worker_id)] = mock_websocket
        
        task_data: dict[str, Any] = {"command": "start_task", "task_id": 456, "priority": "high"}
        
        with patch('builtins.print') as mock_print:
            await ws_service.send_to_worker(worker_id, task_data)
            
            mock_websocket.send_json.assert_called_once_with(task_data)
            mock_print.assert_called_with(f"Sent to {worker_id}: {task_data}")

    @pytest.mark.asyncio
    async def test_connection_tracks_multiple_workers(
        self,
        ws_service: MasterNodeWebsocketServerService,
    ):
        """
        Test that service can track multiple worker connections simultaneously
        """
        
        worker_ids = [UUID(str(uuid4())) for _ in range(3)]
        mock_websockets = [Mock() for _ in range(3)]
        
        for worker_id, mock_ws in zip(worker_ids, mock_websockets):
            mock_ws.send_json = AsyncMock()
            ws_service.connections[str(worker_id)] = mock_ws
        
        # Verify all connections are tracked
        assert len(ws_service.connections) == 3
        for worker_id in worker_ids:
            assert str(worker_id) in ws_service.connections
        
        test_data = {"broadcast": "test_message"}
        for worker_id in worker_ids:
            await ws_service.send_to_worker(worker_id, test_data)
        
        for mock_ws in mock_websockets:
            mock_ws.send_json.assert_called_once_with(test_data)

    @pytest.mark.asyncio
    async def test_json_decode_error_handling_in_connect(
        self,
        ws_service: MasterNodeWebsocketServerService,
        mock_websocket: Mock,
    ):
        worker_id = UUID(str(uuid4()))
        
        from fastapi import WebSocketDisconnect
        from json import JSONDecodeError
        
        mock_websocket.receive_json.side_effect = [
            JSONDecodeError("Invalid JSON", "invalid", 0),
            {"type": "heartbeat"},
            WebSocketDisconnect()
        ]
        
        with patch("builtins.print") as mock_print:
            await ws_service.connect(worker_id, mock_websocket)
            
            mock_print.assert_any_call(f"Invalid JSON from worker: {worker_id}")
            mock_websocket.send_json.assert_called_with({"error": "Invalid JSON format"})

WS_URL = "ws://localhost:8000/ws/connect/{}"

class TestWebsocketIntegration:
    """
    Integration Tests that verify actual websocket behavior over the network
    """
    
    @pytest.fixture
    def worker_id(self):
        return str(uuid4())
    
    @pytest.mark.asyncio
    async def test_websocket_connection_establishes_and_accepts_messages(
        self,
        worker_id: str
    ):
        uri = WS_URL.format(worker_id)
        
        async with websockets.connect(uri) as websocket:
            assert websocket.state.name == "OPEN"
            
            now = datetime.now(timezone.utc)
            messages: List[dict[str, Any]] = [
                {"type": "heartbeat", "timestamp": str(now)},
                {"type": "status_update", "status": "idle"},
                {"type": "task_result", "data": {"result": "success", "task_id": 123}}
            ]
            
            for message in messages:
                await websocket.send(json.dumps(message))
                await asyncio.sleep(0.1)
                
                assert websocket.state.name == "OPEN"
    
    @pytest.mark.asyncio
    async def test_server_reponds_correctly_to_invalid_json(
        self,
        worker_id: str
    ):
        uri = WS_URL.format(worker_id)
        
        async with websockets.connect(uri) as websocket:
            await websocket.send("not a valid json at all")
            
            response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
            data = json.loads(response)
            
            assert "error" in data
            assert data["error"] == "Invalid JSON format"
            
            assert websocket.state.name == "OPEN"
    
    @pytest.mark.asyncio
    async def test_server_responds_correctly_to_missing_type(
        self,
        worker_id: str,
    ):
        uri = WS_URL.format(worker_id)
        
        async with websockets.connect(uri) as websocket:
            await websocket.send(json.dumps({"data": "some data", "timestamp": "now"}))
            
            response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
            data = json.loads(response)
            
            assert "error" in data
            assert data["error"] == "Missing 'type' in message"
            
            assert websocket.state.name == "OPEN"
    
    @pytest.mark.asyncio
    async def test_server_responds_correctly_to_unknown_type(
        self,
        worker_id: str,
    ):
        uri = WS_URL.format(worker_id)
        
        async with websockets.connect(uri) as websocket:
            await websocket.send(json.dumps({"type": "completely_unknown_message", "data": "test"}))
            
            response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
            data = json.loads(response)
            
            assert "error" in data
            assert data["error"] == "Unknown message type"
            
            assert websocket.state.name == "OPEN"
    
    @pytest.mark.asyncio
    async def test_multiple_workers_operate_independently(self):
        worker_ids = [str(uuid4()) for _ in range(3)]
        connections: List[Any] = []
        
        try:
            for worker_id in worker_ids:
                uri = WS_URL.format(worker_id)
                websocket = await websockets.connect(uri)
                connections.append((worker_id, websocket))
                assert websocket.state.name == "OPEN"
                
            message_types = ["heartbeat", "status_update", "task_result"]
            for i, (worker_id, websocket) in enumerate(connections):
                message: dict[str, Any] = {
                    "type": message_types[i],
                    "worker_id": worker_id,
                    "data": f"message_from_worker_{i}"
                }
                await websocket.send(json.dumps(message))
                
                assert websocket.state.name == "OPEN"
                
            await asyncio.sleep(0.5)
            
            for _, websocket in connections:
                assert websocket.state.name == "OPEN"
        finally:
            for _, websocket in connections:
                if websocket.state.name == "OPEN":
                    await websocket.close()
                    
class TestWebsocketServiceBehaviorScenarios:
    """
    Test complex behavioral scenarios that might happen IRL
    """
    
    @pytest.mark.asyncio
    async def test_service_can_send_arbitrary_json_to_specific_worker(self):
        """
        Test the core requirement: sending arbitrary JSON to specific worker by ID
        """
        
        ws_service = MasterNodeWebsocketServerService()
        worker_id = UUID(str(uuid4()))
        
        mock_websocket = Mock()
        mock_websocket.send_json = AsyncMock()
        ws_service.connections[str(worker_id)] = mock_websocket
        
        test_payloads: List[Any] = [
            {"job_id": "job_123", "command": "start", "priority": "high"},
            {"job_id": "job_456", "command": "stop", "reason": "user_request"},
            {"broadcast": "server_maintenance", "scheduled_time": "2025-08-19T15:00:00Z"},
            {"config_update": {"max_connections": 100, "timeout": 30}}
        ]
        
        for payloads in test_payloads:
            await ws_service.send_to_worker(worker_id, payloads)
            mock_websocket.send_json.assert_called_with(payloads)
        
        assert mock_websocket.send_json.call_count == len(test_payloads)
    
    @pytest.mark.asyncio
    async def test_service_handles_worker_message_processing_workflow(self):
        """
        Test the complete workflow of receiving and processing worker messages
        """
        
        ws_service = MasterNodeWebsocketServerService()
        worker_id = UUID(str(uuid4()))
        
        test_scenarios: List[Any] = [
            {
                "message": {"type": "heartbeat", "worker_status": "healthy", "last_task": None},
                "expected_behavior": "logs heartbeat"
            },
            {
                "message": {"type": "task_result", "data": {"job_id": "job_123", "status": "completed", "output": "success"}},
                "expected_behavior": "logs task result data"
            },
            {
                "message": {"type": "status_update", "status": "busy", "current_task": "job_456"},
                "expected_behavior": "logs status update"
            }
        ]
        
        with patch('builtins.print') as mock_print:
            for scenario in test_scenarios:
                await ws_service.handle_worker_message(worker_id, scenario["message"])
        
        assert mock_print.call_count == len(test_scenarios)
    
    @pytest.mark.asyncio
    async def test_rapid_message_sequence_processing(self):
        """
        Test that server can handle rapid sequences of messages without issues
        """

        worker_id = str(uuid4())
        uri = WS_URL.format(worker_id)
        
        async with websockets.connect(uri) as websocket:
            messages: List[Any] = []
            now = datetime.now(timezone.utc)
            for i in range(20):
                message_type = ["heartbeat", "status_update", "task_result"][i % 3]
                message: dict[str, Any] = {
                    "type": message_type,
                    "sequence": i,
                    "timestamp": str(now)
                }
                
                messages.append(message)
            
            for message in messages:
                await websocket.send(json.dumps(message))
            
            await asyncio.sleep(1.0)
            assert websocket.state.name == "OPEN"
    
    @pytest.mark.asyncio
    async def test_error_handling_does_not_break_connection(self):
        """
        Test that various error conditions don't break the WebSocket connection
        """
        
        worker_id = str(uuid4())
        uri = WS_URL.format(worker_id)
        
        async with websockets.connect(uri) as websocket:
            error_scenarios = [
                ("invalid json", "Invalid JSON format"),
                (json.dumps({"no_type": "field"}), "Missing 'type' in message"),
                (json.dumps({"type": "invalid_type"}), "Unknown message type")
            ]
            
            for invalid_message, expected_error in error_scenarios:
                await websocket.send(invalid_message)
                response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                data = json.loads(response)
                assert data["error"] == expected_error
                
                await websocket.send(json.dumps({"type": "heartbeat", "recovery_test": True}))
                await asyncio.sleep(0.1)
                
                assert websocket.state.name == "OPEN"