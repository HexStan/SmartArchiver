from src.core.registry import _registry, register_handler, get_handler_class
from src.core.handlers.standard import StandardHandler
from src.core.handlers.rotate import RotateHandler
from src.core.handlers.sync import SyncHandler


class TestHandlerRegistration:
    def test_all_six_modes_registered(self):
        assert "move" in _registry
        assert "copy" in _registry
        assert "whitelist_move" in _registry
        assert "whitelist_copy" in _registry
        assert "rotate" in _registry
        assert "sync" in _registry

    def test_mode_maps_to_correct_class(self):
        assert _registry["move"] is StandardHandler
        assert _registry["copy"] is StandardHandler
        assert _registry["whitelist_move"] is StandardHandler
        assert _registry["whitelist_copy"] is StandardHandler
        assert _registry["rotate"] is RotateHandler
        assert _registry["sync"] is SyncHandler

    def test_get_handler_class_valid(self):
        assert get_handler_class("move") is StandardHandler
        assert get_handler_class("sync") is SyncHandler
        assert get_handler_class("rotate") is RotateHandler

    def test_get_handler_class_invalid(self):
        assert get_handler_class("nonexistent") is None
        assert get_handler_class("") is None

    def test_register_handler_decorator(self):
        @register_handler("test_mode_x", "test_mode_y")
        class _TestHandler:
            pass

        assert _registry["test_mode_x"] is _TestHandler
        assert _registry["test_mode_y"] is _TestHandler

    def test_registry_has_exactly_six_modes_after_decorator_test(self):
        expected_keys = {
            "move",
            "copy",
            "whitelist_move",
            "whitelist_copy",
            "rotate",
            "sync",
        }
        actual_keys = set(_registry.keys())
        assert expected_keys <= actual_keys
