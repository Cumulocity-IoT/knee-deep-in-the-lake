import json
import fastavro
from pathlib import Path
from IPython.display import display, HTML
from datetime import datetime

def inspect_iceberg_table(table) -> None:
    """
    Provides comprehensive HTML visualization of Iceberg table structure and metadata.

    Args:
        table: PyIceberg Table object
    """
    metadata = table.metadata
    snapshots = list(table.history())
    current_snapshot = table.current_snapshot()

    html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 1400px;">
        <h2 style="color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px;">
            🧊 Iceberg Table: {table.name()}
        </h2>

        <div style="background-color: #ecf0f1; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <h3 style="color: #34495e; margin-top: 0;">Table Overview</h3>
            <table style="width: 100%; border-collapse: collapse;">
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold; width: 200px;">Table UUID</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;"><code>{metadata.table_uuid}</code></td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Location</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;"><code>{metadata.location}</code></td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Format Version</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{metadata.format_version}</td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Last Updated</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{datetime.fromtimestamp(metadata.last_updated_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')}</td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Current Snapshot ID</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;"><code>{metadata.current_snapshot_id}</code></td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Total Snapshots</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{len(snapshots)}</td>
                </tr>
            </table>
        </div>

        <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 5px;">
            📸 Current Snapshot
        </h3>
    """

    if current_snapshot:
        html += f"""
        <div style="background-color: #e8f8f5; padding: 15px; border-radius: 5px; margin-bottom: 20px; border-left: 4px solid #27ae60;">
            <table style="width: 100%; border-collapse: collapse;">
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold; width: 200px;">Snapshot ID</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;"><code>{current_snapshot.snapshot_id}</code></td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Timestamp</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{datetime.fromtimestamp(current_snapshot.timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')}</td>
                </tr>
        """

        if hasattr(current_snapshot, 'summary') and current_snapshot.summary:
            summary = current_snapshot.summary.additional_properties
            if summary:
                html += """
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Summary</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">
                """
                for key, value in sorted(summary.items()):
                    html += f"<strong>{key}:</strong> {value}<br>"
                html += """
                    </td>
                </tr>
                """

        html += """
            </table>
        </div>
        """

    # Current Schema
    current_schema = metadata.schemas[-1] if metadata.schemas else None
    if current_schema:
        html += f"""
        <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 5px;">
            📋 Current Schema (ID: {current_schema.schema_id})
        </h3>
        <div style="background-color: #fff5e6; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
        """

        for field in current_schema.fields:
            field_type = str(field.field_type)
            html += f"""
            <div style="margin: 8px 0; padding: 8px; background-color: #fff; border-left: 3px solid #f39c12; border-radius: 3px;">
                <code style="font-weight: bold; color: #2c3e50;">{field.name}</code>
                <div style="margin-top: 5px; font-size: 0.9em;">
                    ID: <code>{field.field_id}</code> |
                    Type: <code>{field_type}</code> |
                    Required: <strong>{'Yes' if field.required else 'No'}</strong>
                </div>
            </div>
            """

        html += """
        </div>
        """

    # Partition Spec
    if metadata.partition_specs:
        current_spec = metadata.partition_specs[-1]
        html += f"""
        <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 5px;">
            🗂️ Current Partition Spec (ID: {current_spec.spec_id})
        </h3>
        <div style="background-color: #f0f8ff; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
        """

        if current_spec.fields:
            for field in current_spec.fields:
                html += f"""
                <div style="margin: 8px 0; padding: 8px; background-color: #fff; border-left: 3px solid #3498db; border-radius: 3px;">
                    <code style="font-weight: bold;">{field.name}</code>: {field.transform}
                </div>
                """
        else:
            html += "<p><em>Unpartitioned table</em></p>"

        html += """
        </div>
        """

    # Snapshot History
    html += """
    <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 5px;">
        📜 Snapshot History
    </h3>
    """

    for i, snapshot in enumerate(reversed(snapshots)):
        is_current = snapshot.snapshot_id == metadata.current_snapshot_id
        border_color = "#27ae60" if is_current else "#95a5a6"
        bg_color = "#e8f8f5" if is_current else "#f9f9f9"

        html += f"""
        <div style="background-color: {bg_color}; padding: 12px; border-radius: 5px; margin-bottom: 10px; border-left: 4px solid {border_color};">
            <div style="font-weight: bold; color: #2c3e50;">
                Snapshot {len(snapshots) - i}: <code>{snapshot.snapshot_id}</code>
                {'<span style="background-color: #27ae60; color: white; padding: 2px 8px; border-radius: 3px; font-size: 0.85em; margin-left: 10px;">CURRENT</span>' if is_current else ''}
            </div>
            <div style="margin-top: 5px; font-size: 0.9em;">
                <strong>Time:</strong> {datetime.fromtimestamp(snapshot.timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')}<br>
        """

        if hasattr(snapshot, 'summary') and snapshot.summary:
            html += f"<strong>Operation:</strong> {snapshot.summary.operation.value}<br>"
            if snapshot.summary.additional_properties:
                html += "<strong>Changes:</strong> "
                changes = []
                props = snapshot.summary.additional_properties
                if 'added-records' in props:
                    changes.append(f"+{props['added-records']} records")
                if 'deleted-records' in props:
                    changes.append(f"-{props['deleted-records']} records")
                if 'added-data-files' in props:
                    changes.append(f"+{props['added-data-files']} files")
                html += ", ".join(changes) if changes else "N/A"

        html += """
            </div>
        </div>
        """

    html += """
    </div>
    """

    display(HTML(html))


def inspect_metadata_json(json_path: Path) -> None:
    """
    Pretty-print and explain Iceberg metadata JSON structure.

    Args:
        json_path: Path to metadata JSON file
    """
    with open(json_path, 'r') as f:
        metadata = json.load(f)

    html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 1400px;">
        <h2 style="color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px;">
            📄 Metadata JSON: {json_path.name}
        </h2>

        <div style="background-color: #ecf0f1; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <h3 style="color: #34495e; margin-top: 0;">File Overview</h3>
            <table style="width: 100%; border-collapse: collapse;">
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold; width: 200px;">Format Version</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{metadata.get('format-version', 'N/A')}</td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Table UUID</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;"><code>{metadata.get('table-uuid', 'N/A')}</code></td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Location</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;"><code style="font-size: 0.85em;">{metadata.get('location', 'N/A')}</code></td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Last Updated</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{datetime.fromtimestamp(metadata.get('last-updated-ms', 0) / 1000).strftime('%Y-%m-%d %H:%M:%S')}</td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Current Snapshot</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;"><code>{metadata.get('current-snapshot-id', 'N/A')}</code></td>
                </tr>
            </table>
        </div>

        <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 5px;">
            📐 Schemas ({len(metadata.get('schemas', []))})
        </h3>
    """

    for schema in metadata.get('schemas', []):
        html += f"""
        <details style="margin-bottom: 15px;">
            <summary style="cursor: pointer; background-color: #fff5e6; padding: 10px; border-radius: 5px; border-left: 4px solid #f39c12;">
                <strong>Schema ID {schema['schema-id']}</strong> ({len(schema.get('fields', []))} fields)
            </summary>
            <div style="margin-top: 10px; margin-left: 20px;">
        """

        for field in schema.get('fields', []):
            html += f"""
                <div style="margin: 5px 0; padding: 8px; background-color: #fff; border-left: 3px solid #f39c12; border-radius: 3px;">
                    <code style="font-weight: bold;">{field['name']}</code> (ID: {field['id']}) -
                    Type: <code>{field['type']}</code>,
                    Required: <strong>{'Yes' if field.get('required', True) else 'No'}</strong>
                </div>
            """

        html += """
            </div>
        </details>
        """

    # Partition Specs
    html += f"""
    <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 5px;">
        🗂️ Partition Specs ({len(metadata.get('partition-specs', []))})
    </h3>
    """

    for spec in metadata.get('partition-specs', []):
        html += f"""
        <div style="background-color: #f0f8ff; padding: 12px; border-radius: 5px; margin-bottom: 10px; border-left: 4px solid #3498db;">
            <strong>Spec ID {spec['spec-id']}</strong><br>
        """

        if spec.get('fields'):
            html += "<div style='margin-top: 8px;'>"
            for field in spec['fields']:
                html += f"<div style='margin: 3px 0;'><code>{field['name']}</code>: {field.get('transform', 'identity')}</div>"
            html += "</div>"
        else:
            html += "<em>Unpartitioned</em>"

        html += """
        </div>
        """

    # Snapshots
    html += f"""
    <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 5px;">
        📸 Snapshots ({len(metadata.get('snapshots', []))})
    </h3>
    """

    for snapshot in metadata.get('snapshots', []):
        is_current = snapshot['snapshot-id'] == metadata.get('current-snapshot-id')
        border_color = "#27ae60" if is_current else "#95a5a6"

        html += f"""
        <details style="margin-bottom: 10px;">
            <summary style="cursor: pointer; background-color: #{'e8f8f5' if is_current else 'f9f9f9'}; padding: 10px; border-radius: 5px; border-left: 4px solid {border_color};">
                <strong>Snapshot {snapshot['snapshot-id']}</strong>
                {'<span style="background-color: #27ae60; color: white; padding: 2px 8px; border-radius: 3px; font-size: 0.85em; margin-left: 10px;">CURRENT</span>' if is_current else ''}
            </summary>
            <div style="margin-top: 10px; margin-left: 20px;">
                <table style="width: 100%; font-size: 0.9em; border-collapse: collapse;">
                    <tr>
                        <td style="padding: 5px; font-weight: bold; width: 200px;">Timestamp:</td>
                        <td style="padding: 5px;">{datetime.fromtimestamp(snapshot['timestamp-ms'] / 1000).strftime('%Y-%m-%d %H:%M:%S')}</td>
                    </tr>
                    <tr>
                        <td style="padding: 5px; font-weight: bold;">Manifest List:</td>
                        <td style="padding: 5px;"><code style="font-size: 0.85em;">{Path(snapshot.get('manifest-list', '')).name}</code></td>
                    </tr>
                    <tr>
                        <td style="padding: 5px; font-weight: bold;">Schema ID:</td>
                        <td style="padding: 5px;">{snapshot.get('schema-id', 'N/A')}</td>
                    </tr>
        """

        if snapshot.get('summary'):
            html += """
                    <tr>
                        <td style="padding: 5px; font-weight: bold; vertical-align: top;">Summary:</td>
                        <td style="padding: 5px;">
            """
            for key, value in sorted(snapshot['summary'].items()):
                html += f"<div><strong>{key}:</strong> {value}</div>"
            html += """
                        </td>
                    </tr>
            """

        html += """
                </table>
            </div>
        </details>
        """

    # Refs (branches/tags)
    if metadata.get('refs'):
        html += """
        <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 5px;">
            🏷️ References (Branches & Tags)
        </h3>
        """

        for ref_name, ref_data in metadata['refs'].items():
            html += f"""
            <div style="background-color: #f0f4ff; padding: 10px; border-radius: 5px; margin-bottom: 10px; border-left: 4px solid #b8c5e8;">
                <strong>{ref_name}</strong>: {ref_data.get('type', 'unknown')} →
                Snapshot <code>{ref_data.get('snapshot-id', 'N/A')}</code>
            </div>
            """

    html += """
    </div>
    """

    display(HTML(html))
    return metadata


def inspect_manifest(manifest_path: Path):
    """
    Read and display AVRO manifest file contents.

    Args:
        manifest_path: Path to AVRO manifest file

    Returns:
        List of data file paths referenced in this manifest
    """

    with open(manifest_path, 'rb') as f:
        reader = fastavro.reader(f)
        records = list(reader)

    html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 1400px;">
        <h2 style="color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px;">
            📦 Manifest File: {manifest_path.name}
        </h2>

        <div style="background-color: #ecf0f1; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <h3 style="color: #34495e; margin-top: 0;">Overview</h3>
            <table style="width: 100%; border-collapse: collapse;">
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold; width: 200px;">Total Entries</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{len(records)}</td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">File Size</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{manifest_path.stat().st_size:,} bytes</td>
                </tr>
            </table>
        </div>

        <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 5px;">
            📄 Data Files ({len(records)})
        </h3>
    """

    data_file_paths = []

    for i, record in enumerate(records, 1):
        status = record.get('status', 0)
        status_name = {0: 'EXISTING', 1: 'ADDED', 2: 'DELETED'}.get(status, 'UNKNOWN')
        status_color = {'EXISTING': '#95a5a6', 'ADDED': '#27ae60', 'DELETED': '#e74c3c'}.get(status_name, '#95a5a6')

        data_file = record.get('data_file', {})
        file_path = data_file.get('file_path', 'N/A')
        data_file_paths.append(Path(file_path.replace('file://', '')))

        html += f"""
        <details style="margin-bottom: 10px;">
            <summary style="cursor: pointer; background-color: #f9f9f9; padding: 10px; border-radius: 5px; border-left: 4px solid {status_color};">
                <strong>Entry {i}</strong>: <code style="font-size: 0.85em;">{Path(file_path).name}</code>
                <span style="background-color: {status_color}; color: white; padding: 2px 8px; border-radius: 3px; font-size: 0.85em; margin-left: 10px;">{status_name}</span>
            </summary>
            <div style="margin-top: 10px; margin-left: 20px;">
                <table style="width: 100%; font-size: 0.9em; border-collapse: collapse;">
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold; width: 200px;">File Path:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;"><code style="font-size: 0.85em; word-break: break-all;">{file_path}</code></td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">File Format:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">{data_file.get('file_format', 'N/A')}</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">Record Count:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">{data_file.get('record_count', 0):,}</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">File Size:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">{data_file.get('file_size_in_bytes', 0):,} bytes ({data_file.get('file_size_in_bytes', 0) / 1024 / 1024:.2f} MB)</td>
                    </tr>
        """

        # Partition data
        if data_file.get('partition'):
            html += """
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold; vertical-align: top;">Partition:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">
            """
            for key, value in data_file['partition'].items():
                html += f"<div><strong>{key}:</strong> {value}</div>"
            html += """
                        </td>
                    </tr>
            """

        # Value counts (statistics) - handle both dict and list formats
        if data_file.get('value_counts'):
            html += """
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold; vertical-align: top;">Value Counts:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">
            """
            value_counts = data_file['value_counts']
            if isinstance(value_counts, dict):
                for key, value in sorted(value_counts.items()):
                    html += f"<div><code>{key}</code>: {value:,}</div>"
            else:
                html += f"<div>{len(value_counts)} column(s) with statistics</div>"
            html += """
                        </td>
                    </tr>
            """

        # Lower bounds - handle both dict and list formats
        if data_file.get('lower_bounds'):
            html += """
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold; vertical-align: top;">Lower Bounds:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">
            """
            lower_bounds = data_file['lower_bounds']
            if isinstance(lower_bounds, dict):
                for key, value in sorted(lower_bounds.items()):
                    html += f"<div><code>{key}</code>: {value!r}</div>"
            else:
                html += f"<div>{len(lower_bounds)} column(s) with bounds</div>"
            html += """
                        </td>
                    </tr>
            """

        # Upper bounds - handle both dict and list formats
        if data_file.get('upper_bounds'):
            html += """
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold; vertical-align: top;">Upper Bounds:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">
            """
            upper_bounds = data_file['upper_bounds']
            if isinstance(upper_bounds, dict):
                for key, value in sorted(upper_bounds.items()):
                    html += f"<div><code>{key}</code>: {value!r}</div>"
            else:
                html += f"<div>{len(upper_bounds)} column(s) with bounds</div>"
            html += """
                        </td>
                    </tr>
            """

        html += """
                </table>
            </div>
        </details>
        """

    html += """
    </div>
    """

    display(HTML(html))
    return data_file_paths


def inspect_manifest_list(manifest_list_path: Path, metadata_file_name: str = None, snapshot_id: int = None):
    """
    Read and display AVRO manifest list contents with visualization.

    Args:
        manifest_list_path: Path to AVRO manifest list file
        metadata_file_name: Optional name of metadata file for context
        snapshot_id: Optional snapshot ID for context
    """
    with open(manifest_list_path, 'rb') as f:
        reader = fastavro.reader(f)
        manifest_list_entries = list(reader)

    html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 1400px;">
        <h2 style="color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px;">
            📋 Manifest List: {manifest_list_path.name}
        </h2>

        <div style="background-color: #ecf0f1; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <h3 style="color: #34495e; margin-top: 0;">Overview</h3>
            <table style="width: 100%; border-collapse: collapse;">
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold; width: 200px;">Total Manifests</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{len(manifest_list_entries)}</td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">File Size</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{manifest_list_path.stat().st_size:,} bytes</td>
                </tr>
    """

    if snapshot_id:
        html += f"""
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Snapshot ID</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;"><code>{snapshot_id}</code></td>
                </tr>
        """

    html += """
            </table>
        </div>

        <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 5px;">
            📦 Manifest Files
        </h3>
    """

    for i, entry in enumerate(manifest_list_entries, 1):
        manifest_path = Path(entry['manifest_path'].replace('file://', ''))
        added = entry.get('added_files_count', 0)
        existing = entry.get('existing_files_count', 0)
        deleted = entry.get('deleted_files_count', 0)
        added_rows = entry.get('added_rows_count', 0)
        existing_rows = entry.get('existing_rows_count', 0)
        deleted_rows = entry.get('deleted_rows_count', 0)
        content = entry.get('content', 0)
        content_type = {0: 'DATA', 1: 'DELETES'}.get(content, 'UNKNOWN')

        # Determine border color based on status
        if deleted > 0:
            border_color = '#e74c3c'
        elif added > 0:
            border_color = '#27ae60'
        else:
            border_color = '#95a5a6'

        html += f"""
        <details style="margin-bottom: 10px;">
            <summary style="cursor: pointer; background-color: #f9f9f9; padding: 10px; border-radius: 5px; border-left: 4px solid {border_color};">
                <strong>{i}. {manifest_path.name}</strong>
                <span style="background-color: #3498db; color: white; padding: 2px 8px; border-radius: 3px; font-size: 0.85em; margin-left: 10px;">{content_type}</span>
            </summary>
            <div style="margin-top: 10px; margin-left: 20px;">
                <table style="width: 100%; font-size: 0.9em; border-collapse: collapse;">
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold; width: 200px;">Manifest Length:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">{entry.get('manifest_length', 0):,} bytes</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">Partition Spec ID:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">{entry.get('partition_spec_id', 'N/A')}</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">Content Type:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">{content_type}</td>
                    </tr>
                    <tr style="background-color: #e8f8f5;">
                        <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;" colspan="2">File Counts</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; padding-left: 20px;">Added files:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;"><span style="color: #27ae60; font-weight: bold;">+{added}</span></td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; padding-left: 20px;">Existing files:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">{existing}</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; padding-left: 20px;">Deleted files:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;"><span style="color: #e74c3c; font-weight: bold;">-{deleted}</span></td>
                    </tr>
                    <tr style="background-color: #e8f8f5;">
                        <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;" colspan="2">Row Counts</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; padding-left: 20px;">Added rows:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;"><span style="color: #27ae60; font-weight: bold;">+{added_rows:,}</span></td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; padding-left: 20px;">Existing rows:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">{existing_rows:,}</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; padding-left: 20px;">Deleted rows:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;"><span style="color: #e74c3c; font-weight: bold;">-{deleted_rows:,}</span></td>
                    </tr>
                </table>
            </div>
        </details>
        """

    # Add linkage visualization
    if metadata_file_name and snapshot_id:
        html += f"""
        <div style="margin-top: 30px; background-color: #f5f5f5; padding: 15px; border-radius: 5px;">
            <h3 style="color: #2c3e50; margin-top: 0;">Metadata Linkage</h3>
            <div style="font-family: monospace; line-height: 1.8;">
                <div>📄 {metadata_file_name}</div>
                <div style="margin-left: 20px;">└─ 📸 Snapshot {snapshot_id}</div>
                <div style="margin-left: 40px;">└─ 📋 {manifest_list_path.name}</div>
        """

        for i, entry in enumerate(manifest_list_entries, 1):
            manifest_path = Path(entry['manifest_path'].replace('file://', ''))
            added = entry.get('added_files_count', 0)
            existing = entry.get('existing_files_count', 0)
            deleted = entry.get('deleted_files_count', 0)
            rows = entry.get('added_rows_count', 0) + entry.get('existing_rows_count', 0)

            status = []
            if added > 0:
                status.append(f"+{added} files")
            if existing > 0:
                status.append(f"{existing} existing")
            if deleted > 0:
                status.append(f"-{deleted} files")
            status_str = ", ".join(status) if status else "no changes"

            connector = "├─" if i < len(manifest_list_entries) else "└─"
            html += f'<div style="margin-left: 60px;">{connector} 📦 {manifest_path.name}</div>\n'
            html += f'<div style="margin-left: 60px; color: #666;">{"│" if i < len(manifest_list_entries) else " "}   ({status_str}, {rows:,} rows)</div>\n'

        html += """
            </div>
        </div>
        """

    html += """
    </div>
    """

    display(HTML(html))
    return manifest_list_entries


def compare_snapshots(table, snapshot_id_1: int, snapshot_id_2: int) -> None:
    """
    Show differences between two snapshots.

    Args:
        table: PyIceberg Table object
        snapshot_id_1: First snapshot ID
        snapshot_id_2: Second snapshot ID
    """
    snap1 = table.snapshot_by_id(snapshot_id_1)
    snap2 = table.snapshot_by_id(snapshot_id_2)

    if not snap1 or not snap2:
        print("❌ One or both snapshots not found")
        return

    html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 1400px;">
        <h2 style="color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px;">
            ⚖️ Snapshot Comparison
        </h2>

        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px;">
            <div style="background-color: #e8f4fd; padding: 15px; border-radius: 5px; border-left: 4px solid #3498db;">
                <h3 style="color: #2c3e50; margin-top: 0;">Snapshot 1</h3>
                <table style="width: 100%; font-size: 0.9em;">
                    <tr>
                        <td style="padding: 5px; font-weight: bold;">ID:</td>
                        <td style="padding: 5px;"><code>{snap1.snapshot_id}</code></td>
                    </tr>
                    <tr>
                        <td style="padding: 5px; font-weight: bold;">Time:</td>
                        <td style="padding: 5px;">{datetime.fromtimestamp(snap1.timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')}</td>
                    </tr>
    """

    if snap1.summary:
        html += f"<tr><td style='padding: 5px; font-weight: bold;'>Operation:</td><td style='padding: 5px;'>{snap1.summary.operation.value}</td></tr>"
        if snap1.summary.additional_properties:
            for key, value in sorted(snap1.summary.additional_properties.items()):
                html += f"<tr><td style='padding: 5px; font-weight: bold;'>{key}:</td><td style='padding: 5px;'>{value}</td></tr>"

    html += """
                </table>
            </div>

            <div style="background-color: #e8f4fd; padding: 15px; border-radius: 5px; border-left: 4px solid #3498db;">
                <h3 style="color: #2c3e50; margin-top: 0;">Snapshot 2</h3>
                <table style="width: 100%; font-size: 0.9em;">
    """

    html += f"""
                    <tr>
                        <td style="padding: 5px; font-weight: bold;">ID:</td>
                        <td style="padding: 5px;"><code>{snap2.snapshot_id}</code></td>
                    </tr>
                    <tr>
                        <td style="padding: 5px; font-weight: bold;">Time:</td>
                        <td style="padding: 5px;">{datetime.fromtimestamp(snap2.timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')}</td>
                    </tr>
    """

    if snap2.summary:
        html += f"<tr><td style='padding: 5px; font-weight: bold;'>Operation:</td><td style='padding: 5px;'>{snap2.summary.operation.value}</td></tr>"
        if snap2.summary.additional_properties:
            for key, value in sorted(snap2.summary.additional_properties.items()):
                html += f"<tr><td style='padding: 5px; font-weight: bold;'>{key}:</td><td style='padding: 5px;'>{value}</td></tr>"

    html += """
                </table>
            </div>
        </div>

        <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 5px;">
            📊 Comparison Summary
        </h3>
        <div style="background-color: #fff3cd; padding: 15px; border-radius: 5px; border-left: 4px solid #ffc107;">
    """

    # Calculate differences
    if snap1.summary and snap2.summary:
        props1 = snap1.summary.additional_properties or {}
        props2 = snap2.summary.additional_properties or {}

        records1 = int(props1.get('total-records', props1.get('added-records', 0)))
        records2 = int(props2.get('total-records', props2.get('added-records', 0)))

        files1 = int(props1.get('total-data-files', props1.get('added-data-files', 0)))
        files2 = int(props2.get('total-data-files', props2.get('added-data-files', 0)))

        html += f"""
            <div style="margin: 10px 0;">
                <strong>Record Count Change:</strong>
                {records1:,} → {records2:,}
                <span style="color: {'#27ae60' if records2 >= records1 else '#e74c3c'}; font-weight: bold;">
                    ({'+' if records2 >= records1 else ''}{records2 - records1:,})
                </span>
            </div>
            <div style="margin: 10px 0;">
                <strong>Data Files Change:</strong>
                {files1:,} → {files2:,}
                <span style="color: {'#27ae60' if files2 >= files1 else '#e74c3c'}; font-weight: bold;">
                    ({'+' if files2 >= files1 else ''}{files2 - files1:,})
                </span>
            </div>
        """

    html += """
        </div>
    </div>
    """

    display(HTML(html))
