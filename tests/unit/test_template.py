"""
Unit tests for template resolution.
"""

import pytest

from workflow_engine.template.resolver import (
    TemplateResolver,
    TemplateValidationError,
    resolve_node_input,
)


class TestTemplateResolver:
    """Tests for template resolution."""
    
    def test_simple_reference(self):
        """Test simple node output reference."""
        resolver = TemplateResolver(
            node_outputs={"node1": {"value": 42}},
        )
        
        result = resolver.resolve("{{ node1.value }}")
        
        assert result == 42
    
    def test_nested_reference(self):
        """Test nested path reference."""
        resolver = TemplateResolver(
            node_outputs={
                "api_call": {
                    "response": {
                        "data": {
                            "user": {"name": "John"}
                        }
                    }
                }
            },
        )
        
        result = resolver.resolve("{{ api_call.response.data.user.name }}")
        
        assert result == "John"
    
    def test_string_interpolation(self):
        """Test template embedded in string."""
        resolver = TemplateResolver(
            node_outputs={"user": {"name": "Alice"}},
        )
        
        result = resolver.resolve("Hello, {{ user.name }}!")
        
        assert result == "Hello, Alice!"
    
    def test_multiple_references(self):
        """Test multiple references in one string."""
        resolver = TemplateResolver(
            node_outputs={
                "user": {"name": "Bob"},
                "greeting": {"text": "Welcome"},
            },
        )
        
        result = resolver.resolve("{{ greeting.text }}, {{ user.name }}!")
        
        assert result == "Welcome, Bob!"
    
    def test_input_params_reference(self):
        """Test reference to workflow input params."""
        resolver = TemplateResolver(
            node_outputs={},
            input_params={"user_id": 123},
        )
        
        result = resolver.resolve("User ID: {{ input.user_id }}")
        
        assert result == "User ID: 123"
    
    def test_dict_resolution(self):
        """Test resolution in nested dict."""
        resolver = TemplateResolver(
            node_outputs={"api": {"url": "http://example.com"}},
        )
        
        config = {
            "endpoint": "{{ api.url }}",
            "headers": {"X-Custom": "value"},
        }
        
        result = resolver.resolve(config)
        
        assert result["endpoint"] == "http://example.com"
        assert result["headers"]["X-Custom"] == "value"
    
    def test_list_resolution(self):
        """Test resolution in list."""
        resolver = TemplateResolver(
            node_outputs={"items": {"a": 1, "b": 2}},
        )
        
        data = ["{{ items.a }}", "static", "{{ items.b }}"]
        
        result = resolver.resolve(data)
        
        assert result == [1, "static", 2]
    
    def test_preserve_types(self):
        """Test that types are preserved for single reference."""
        resolver = TemplateResolver(
            node_outputs={
                "data": {
                    "list": [1, 2, 3],
                    "dict": {"key": "value"},
                    "number": 42,
                    "bool": True,
                }
            },
        )
        
        assert resolver.resolve("{{ data.list }}") == [1, 2, 3]
        assert resolver.resolve("{{ data.dict }}") == {"key": "value"}
        assert resolver.resolve("{{ data.number }}") == 42
        assert resolver.resolve("{{ data.bool }}") is True
    
    def test_nonexistent_node_returns_none(self):
        """Test that nonexistent node reference returns None."""
        resolver = TemplateResolver(node_outputs={})
        
        result = resolver.resolve("{{ missing.value }}")
        
        assert result is None
    
    def test_nonexistent_path_returns_none(self):
        """Test that nonexistent path returns None."""
        resolver = TemplateResolver(
            node_outputs={"node": {"exists": True}},
        )
        
        result = resolver.resolve("{{ node.missing.deep.path }}")
        
        assert result is None
    
    def test_no_templates_passthrough(self):
        """Test that strings without templates pass through."""
        resolver = TemplateResolver(node_outputs={})
        
        result = resolver.resolve("Just a plain string")
        
        assert result == "Just a plain string"
    
    def test_array_index_access(self):
        """Test array index access in path."""
        resolver = TemplateResolver(
            node_outputs={
                "data": {"items": ["first", "second", "third"]}
            },
        )
        
        result = resolver.resolve("{{ data.items[1] }}")
        
        assert result == "second"
    
    # Edge cases
    def test_empty_string(self):
        """Test empty string passes through."""
        resolver = TemplateResolver(node_outputs={})
        
        result = resolver.resolve("")
        
        assert result == ""
    
    def test_whitespace_only_string(self):
        """Test whitespace-only string passes through."""
        resolver = TemplateResolver(node_outputs={})
        
        result = resolver.resolve("   ")
        
        assert result == "   "
    
    def test_template_with_extra_whitespace(self):
        """Test template with extra whitespace in braces."""
        resolver = TemplateResolver(
            node_outputs={"node": {"value": 42}},
        )
        
        result = resolver.resolve("{{   node.value   }}")
        
        assert result == 42
    
    def test_array_index_zero(self):
        """Test array index 0."""
        resolver = TemplateResolver(
            node_outputs={"data": {"items": ["first", "second"]}},
        )
        
        result = resolver.resolve("{{ data.items[0] }}")
        
        assert result == "first"
    
    def test_array_index_out_of_bounds(self):
        """Test array index out of bounds returns None."""
        resolver = TemplateResolver(
            node_outputs={"data": {"items": ["first", "second"]}},
        )
        
        result = resolver.resolve("{{ data.items[99] }}")
        
        assert result is None
    
    def test_deeply_nested_dict(self):
        """Test deeply nested dictionary access."""
        nested = {"level1": {"level2": {"level3": {"level4": {"value": "deep"}}}}}
        resolver = TemplateResolver(node_outputs={"data": nested})
        
        result = resolver.resolve("{{ data.level1.level2.level3.level4.value }}")
        
        assert result == "deep"
    
    def test_null_value_in_output(self):
        """Test handling of null/None values in outputs."""
        resolver = TemplateResolver(
            node_outputs={"node": {"nullable": None}},
        )
        
        result = resolver.resolve("Value: {{ node.nullable }}")
        
        assert result == "Value: "
    
    def test_boolean_false_value(self):
        """Test boolean False value is preserved."""
        resolver = TemplateResolver(
            node_outputs={"node": {"flag": False}},
        )
        
        result = resolver.resolve("{{ node.flag }}")
        
        assert result is False
    
    def test_integer_zero_value(self):
        """Test integer 0 value is preserved."""
        resolver = TemplateResolver(
            node_outputs={"node": {"count": 0}},
        )
        
        result = resolver.resolve("{{ node.count }}")
        
        assert result == 0
    
    def test_empty_list_value(self):
        """Test empty list value is preserved."""
        resolver = TemplateResolver(
            node_outputs={"node": {"items": []}},
        )
        
        result = resolver.resolve("{{ node.items }}")
        
        assert result == []
    
    def test_empty_dict_value(self):
        """Test empty dict value is preserved."""
        resolver = TemplateResolver(
            node_outputs={"node": {"data": {}}},
        )
        
        result = resolver.resolve("{{ node.data }}")
        
        assert result == {}
    
    def test_unicode_in_values(self):
        """Test unicode characters in values."""
        resolver = TemplateResolver(
            node_outputs={"node": {"name": "日本語テスト"}},
        )
        
        result = resolver.resolve("Name: {{ node.name }}")
        
        assert result == "Name: 日本語テスト"
    
    def test_special_chars_in_values(self):
        """Test special characters in values."""
        resolver = TemplateResolver(
            node_outputs={"node": {"text": "Hello <script>alert('xss')</script>"}},
        )
        
        result = resolver.resolve("{{ node.text }}")
        
        # Should preserve the value as-is (no escaping by default)
        assert result == "Hello <script>alert('xss')</script>"
    
    def test_newlines_in_values(self):
        """Test newlines in values."""
        resolver = TemplateResolver(
            node_outputs={"node": {"multiline": "line1\nline2\nline3"}},
        )
        
        result = resolver.resolve("{{ node.multiline }}")
        
        assert result == "line1\nline2\nline3"
    
    def test_env_vars_reference(self):
        """Test environment variable reference."""
        resolver = TemplateResolver(
            node_outputs={},
            env_vars={"API_KEY": "secret123"},
        )
        
        result = resolver.resolve("Key: {{ env.API_KEY }}")
        
        assert result == "Key: secret123"
    
    def test_nested_dict_resolution(self):
        """Test deeply nested dict resolution."""
        resolver = TemplateResolver(
            node_outputs={"node": {"value": "test"}},
        )
        
        config = {
            "level1": {
                "level2": {
                    "level3": "{{ node.value }}"
                }
            }
        }
        
        result = resolver.resolve(config)
        
        assert result["level1"]["level2"]["level3"] == "test"
    
    def test_nested_list_resolution(self):
        """Test nested list resolution."""
        resolver = TemplateResolver(
            node_outputs={"node": {"a": 1, "b": 2}},
        )
        
        config = [
            ["{{ node.a }}", "{{ node.b }}"],
            ["static"]
        ]
        
        result = resolver.resolve(config)
        
        assert result == [[1, 2], ["static"]]
    
    def test_mixed_nested_structure(self):
        """Test mixed dict/list nested structure."""
        resolver = TemplateResolver(
            node_outputs={"node": {"x": 1, "y": 2}},
        )
        
        config = {
            "items": [
                {"value": "{{ node.x }}"},
                {"value": "{{ node.y }}"}
            ]
        }
        
        result = resolver.resolve(config)
        
        assert result["items"][0]["value"] == 1
        assert result["items"][1]["value"] == 2
    
    def test_non_string_passthrough(self):
        """Test non-string types pass through unchanged."""
        resolver = TemplateResolver(node_outputs={})
        
        assert resolver.resolve(42) == 42
        assert resolver.resolve(3.14) == 3.14
        assert resolver.resolve(True) is True
        assert resolver.resolve(None) is None
    
    def test_malformed_reference_no_match(self):
        """Test malformed reference (unclosed braces) passes through."""
        resolver = TemplateResolver(
            node_outputs={"node": {"value": 42}},
        )
        
        # Unclosed brace - should pass through as-is
        result = resolver.resolve("{{ node.value")
        
        assert result == "{{ node.value"
    
    def test_single_brace_passthrough(self):
        """Test single braces pass through."""
        resolver = TemplateResolver(node_outputs={})
        
        result = resolver.resolve("{ not a template }")
        
        assert result == "{ not a template }"
    
    def test_empty_braces_passthrough(self):
        """Test empty braces pass through."""
        resolver = TemplateResolver(node_outputs={})
        
        result = resolver.resolve("{{  }}")
        
        # Empty reference should not match valid pattern
        assert result == "{{  }}"
    
    def test_triple_braces_passthrough(self):
        """Test triple braces pattern."""
        resolver = TemplateResolver(
            node_outputs={"node": {"value": 42}},
        )
        
        result = resolver.resolve("{{{ node.value }}}")
        
        # Should resolve inner template
        assert "42" in str(result)
    
    def test_reference_in_middle_of_long_string(self):
        """Test template reference in middle of long string."""
        resolver = TemplateResolver(
            node_outputs={"user": {"id": 123}},
        )
        
        long_text = "Start of text. " * 10 + "User ID: {{ user.id }}" + " End of text." * 10
        result = resolver.resolve(long_text)
        
        assert "User ID: 123" in result
    
    def test_many_references_in_string(self):
        """Test many template references in one string."""
        outputs = {f"node{i}": {"val": i} for i in range(10)}
        resolver = TemplateResolver(node_outputs=outputs)
        
        template = " ".join([f"{{{{ node{i}.val }}}}" for i in range(10)])
        result = resolver.resolve(template)
        
        assert result == "0 1 2 3 4 5 6 7 8 9"
    
    def test_node_id_with_underscore(self):
        """Test node ID with underscore."""
        resolver = TemplateResolver(
            node_outputs={"my_node": {"value": "test"}},
        )
        
        result = resolver.resolve("{{ my_node.value }}")
        
        assert result == "test"
    
    def test_node_id_with_hyphen(self):
        """Test node ID with hyphen."""
        resolver = TemplateResolver(
            node_outputs={"my-node": {"value": "test"}},
        )
        
        result = resolver.resolve("{{ my-node.value }}")
        
        assert result == "test"
    
    def test_input_nested_access(self):
        """Test nested access in input params."""
        resolver = TemplateResolver(
            node_outputs={},
            input_params={"config": {"setting": "enabled"}},
        )
        
        result = resolver.resolve("{{ input.config.setting }}")
        
        assert result == "enabled"


class TestTemplateValidation:
    """Tests for template validation."""
    
    def test_find_references(self):
        """Test finding all template references."""
        resolver = TemplateResolver(node_outputs={})
        
        refs = resolver.find_references(
            "Start {{ node1.value }} middle {{ node2.data.field }} end"
        )
        
        assert len(refs) == 2
        assert refs[0].node_id == "node1"
        assert refs[0].path == ["value"]
        assert refs[1].node_id == "node2"
        assert refs[1].path == ["data", "field"]
    
    # Edge cases
    def test_find_references_empty_string(self):
        """Test find_references with empty string."""
        resolver = TemplateResolver(node_outputs={})
        
        refs = resolver.find_references("")
        
        assert len(refs) == 0
    
    def test_find_references_no_templates(self):
        """Test find_references with no templates."""
        resolver = TemplateResolver(node_outputs={})
        
        refs = resolver.find_references("Just plain text")
        
        assert len(refs) == 0
    
    def test_find_references_single_reference(self):
        """Test find_references with single reference."""
        resolver = TemplateResolver(node_outputs={})
        
        refs = resolver.find_references("{{ node.value }}")
        
        assert len(refs) == 1
        assert refs[0].node_id == "node"
    
    def test_find_references_with_array_index(self):
        """Test find_references with array index in path."""
        resolver = TemplateResolver(node_outputs={})
        
        refs = resolver.find_references("{{ node.items[0].name }}")
        
        assert len(refs) == 1
        assert refs[0].node_id == "node"
        # Path should include the array index
        assert "items" in refs[0].path
    
    def test_find_references_input_reference(self):
        """Test find_references with input reference."""
        resolver = TemplateResolver(node_outputs={})
        
        refs = resolver.find_references("{{ input.user_id }}")
        
        assert len(refs) == 1
        assert refs[0].node_id == "input"
        assert refs[0].path == ["user_id"]
    
    def test_find_references_positions(self):
        """Test that reference positions are correctly captured."""
        resolver = TemplateResolver(node_outputs={})
        
        template = "Hello {{ name }}!"
        refs = resolver.find_references(template)
        
        assert len(refs) == 1
        assert refs[0].start_pos == 6
        assert refs[0].end_pos == 17
        assert template[refs[0].start_pos:refs[0].end_pos] == "{{ name }}"
    
    def test_find_references_full_match(self):
        """Test that full_match contains the entire template."""
        resolver = TemplateResolver(node_outputs={})
        
        refs = resolver.find_references("Test {{ node.path.to.value }}")
        
        assert refs[0].full_match == "{{ node.path.to.value }}"


class TestResolveNodeInput:
    """Tests for the convenience function."""
    
    def test_resolve_node_input(self):
        """Test resolve_node_input helper function."""
        config = {
            "url": "http://api.example.com/{{ input.resource }}",
            "headers": {"Authorization": "Bearer {{ auth.token }}"},
        }
        
        completed_outputs = {
            "auth": {"token": "abc123"},
        }
        
        input_params = {
            "resource": "users",
        }
        
        result = resolve_node_input(config, completed_outputs, input_params)
        
        assert result["url"] == "http://api.example.com/users"
        assert result["headers"]["Authorization"] == "Bearer abc123"
