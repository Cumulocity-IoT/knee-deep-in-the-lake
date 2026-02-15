from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
from IPython.display import display, HTML

def inspect(file_path: Path) -> None:
    import struct

    pqfile = pq.ParquetFile(file_path)
    meta = pqfile.metadata

    with open(file_path, 'rb') as f:
        file_bytes = f.read()
    file_size = len(file_bytes)

    # Calculate metadata location (at the end of file)
    metadata_len = struct.unpack('<i', file_bytes[-8:-4])[0]
    metadata_start = file_size - 8 - metadata_len

    # Build HTML output
    html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 1400px;">
        <h2 style="color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px;">
            📄 Parquet File: {file_path}
        </h2>

        <div style="background-color: #ecf0f1; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <h3 style="color: #34495e; margin-top: 0;">File Overview</h3>
            <table style="width: 100%; border-collapse: collapse;">
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold; width: 200px;">Total File Size</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{file_size:,} bytes ({file_size / 1024:.2f} KB)</td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Created By</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{meta.created_by}</td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Format Version</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{meta.format_version}</td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Total Rows</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{meta.num_rows:,}</td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Total Columns</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{meta.num_columns}</td>
                </tr>
                <tr style="background-color: #fff;">
                    <td style="padding: 8px; border: 1px solid #bdc3c7; font-weight: bold;">Row Groups</td>
                    <td style="padding: 8px; border: 1px solid #bdc3c7;">{meta.num_row_groups}</td>
                </tr>
            </table>
        </div>

        <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 5px;">
            📋 File Structure (Physical Layout)
        </h3>

        <div style="background-color: #fff; border-left: 4px solid #3498db; padding: 10px; margin: 20px 0;">
    """

    # 1. HEADER
    html += """
        <div style="margin-bottom: 25px;">
            <h4 style="color: #16a085; margin: 10px 0;">
                1️⃣ Header (Magic Number)
            </h4>
            <div style="margin-left: 20px; background-color: #f0fff4; padding: 15px; border-radius: 5px; border: 1px solid #a8e6cf;">
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold; width: 180px;">Location:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">Bytes 0-3</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">Size:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">4 bytes</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">Content:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;"><code style="background-color: #e8e8e8; padding: 2px 6px; border-radius: 3px;">PAR1</code></td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">Purpose:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">File format identifier</td>
                    </tr>
                </table>
            </div>
        </div>
    """

    # 2. ROW GROUPS (DATA BLOCKS)
    html += """
        <div style="margin-bottom: 25px;">
            <h4 style="color: #16a085; margin: 10px 0;">
                2️⃣ Row Groups (Data Blocks)
            </h4>
    """

    current_offset = 4  # After header

    for rg_idx in range(meta.num_row_groups):
        rg = meta.row_group(rg_idx)

        # Find the start and end of this row group
        # The row group spans from the first byte of any column chunk to the last byte
        min_offset = float('inf')
        max_offset = 0

        for col_idx in range(rg.num_columns):
            col = rg.column(col_idx)

            # Start of column chunk (dictionary page if present, otherwise data page)
            if col.has_dictionary_page:
                col_start = col.dictionary_page_offset
            else:
                col_start = col.data_page_offset

            min_offset = min(min_offset, col_start)

            # End of column chunk
            # The total_compressed_size includes all data pages (headers + data)
            # but does NOT include the dictionary page
            # So the end is: data_page_offset + total_compressed_size
            col_end = col.data_page_offset + col.total_compressed_size

            max_offset = max(max_offset, col_end)

        # The row group ends where the metadata starts (for the last row group)
        # or where the next row group starts (for earlier row groups)
        # But we calculate based on actual column chunk extents
        if rg_idx == meta.num_row_groups - 1:
            # Last row group - it ends where metadata starts
            max_offset = min(max_offset, metadata_start)

        rg_size = max_offset - min_offset

        html += f"""
            <div style="margin-left: 20px; margin-bottom: 20px; background-color: #f0f8ff; padding: 15px; border-radius: 5px; border: 1px solid #b0d4f1;">
                <h5 style="color: #2980b9; margin: 0 0 10px 0;">Row Group {rg_idx}</h5>
                <table style="width: 100%; border-collapse: collapse; margin-bottom: 15px;">
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold; width: 180px;">Location:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">Bytes {min_offset:,} - {max_offset-1:,}</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">Compressed size:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">{rg_size:,} bytes ({rg_size / 1024:.2f} KB)</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">Uncompressed size:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">{rg.total_byte_size:,} bytes</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">Rows:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">{rg.num_rows:,}</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">Columns:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">{rg.num_columns}</td>
                    </tr>
                </table>

                <details style="margin-top: 10px;">
                    <summary style="cursor: pointer; color: #2980b9; font-weight: bold; padding: 5px;">
                        📊 Column Chunks ({rg.num_columns} columns)
                    </summary>
                    <div style="margin-top: 10px;">
        """

        # Column chunks
        for col_idx in range(rg.num_columns):
            col = rg.column(col_idx)
            stats = col.statistics if col.is_stats_set else None

            html += f"""
                        <div style="margin: 10px 0; padding: 10px; background-color: #fff; border-left: 3px solid #3498db; border-radius: 3px;">
                            <div style="font-weight: bold; color: #2c3e50; margin-bottom: 8px;">
                                Column: <code style="background-color: #e8e8e8; padding: 2px 6px; border-radius: 3px;">{col.path_in_schema}</code>
                            </div>
                            <table style="width: 100%; font-size: 0.9em;">
                                <tr>
                                    <td style="padding: 3px; width: 180px;">Physical Type:</td>
                                    <td style="padding: 3px;"><code>{col.physical_type}</code></td>
                                </tr>
                                <tr>
                                    <td style="padding: 3px;">Compression:</td>
                                    <td style="padding: 3px;"><code>{col.compression}</code></td>
                                </tr>
                                <tr>
                                    <td style="padding: 3px;">Encodings:</td>
                                    <td style="padding: 3px;"><code>{', '.join(str(e) for e in col.encodings)}</code></td>
                                </tr>
            """

            if col.has_dictionary_page:
                dict_size = col.data_page_offset - col.dictionary_page_offset
                html += f"""
                                <tr>
                                    <td style="padding: 3px;">Dictionary Page:</td>
                                    <td style="padding: 3px;">Offset {col.dictionary_page_offset:,}, Size {dict_size:,} bytes</td>
                                </tr>
                """

            html += f"""
                                <tr>
                                    <td style="padding: 3px;">Data Pages Start:</td>
                                    <td style="padding: 3px;">Offset {col.data_page_offset:,}</td>
                                </tr>
                                <tr>
                                    <td style="padding: 3px;">Compressed Size:</td>
                                    <td style="padding: 3px;">{col.total_compressed_size:,} bytes</td>
                                </tr>
                                <tr>
                                    <td style="padding: 3px;">Uncompressed Size:</td>
                                    <td style="padding: 3px;">{col.total_uncompressed_size:,} bytes
                                        <span style="color: #27ae60;">({100 * col.total_compressed_size / col.total_uncompressed_size :.0f}%)</span>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding: 3px;">Values:</td>
                                    <td style="padding: 3px;">{col.num_values:,}</td>
                                </tr>
            """

            if stats and stats.has_min_max:
                html += f"""
                                <tr>
                                    <td style="padding: 3px;">Statistics:</td>
                                    <td style="padding: 3px;">
                                        Min: <code>{str(stats.min)[:40]}</code>,
                                        Max: <code>{str(stats.max)[:40]}</code>,
                                        Nulls: {stats.null_count:,}
                                    </td>
                                </tr>
                """

            html += """
                            </table>
                        </div>
            """

        html += """
                    </div>
                </details>
            </div>
        """

    html += """
        </div>
    """

    # 3. FILE METADATA
    html += f"""
        <div style="margin-bottom: 25px;">
            <h4 style="color: #16a085; margin: 10px 0;">
                3️⃣ File Metadata (Thrift Structure)
            </h4>
            <div style="margin-left: 20px; background-color: #fff5e6; padding: 15px; border-radius: 5px; border: 1px solid #f39c12;">
                <table style="width: 100%; border-collapse: collapse;">
                    <tr>
                        <td style="padding: 5px; font-weight: bold; width: 180px;">Location:</td>
                        <td style="padding: 5px;">Bytes {metadata_start:,} - {metadata_start + metadata_len - 1:,}</td>
                    </tr>
                    <tr>
                        <td style="padding: 5px; font-weight: bold;">Size:</td>
                        <td style="padding: 5px;">{metadata_len:,} bytes ({metadata_len / 1024:.2f} KB)</td>
                    </tr>
                    <tr>
                        <td style="padding: 5px; font-weight: bold;">Purpose:</td>
                        <td style="padding: 5px;">Contains schema, row group locations, column statistics, and file properties</td>
                    </tr>
                    <tr>
                        <td style="padding: 5px; font-weight: bold;">Format:</td>
                        <td style="padding: 5px;">Apache Thrift compact binary protocol</td>
                    </tr>
                </table>

                <details style="margin-top: 15px;">
                    <summary style="cursor: pointer; color: #d68910; font-weight: bold; padding: 5px;">
                        📐 Schema Details ({meta.num_columns} columns)
                    </summary>
                    <div style="margin-top: 10px; max-height: 400px; overflow-y: auto;">
    """

    # Schema details
    for i in range(meta.num_columns):
        col_schema = pqfile.schema[i]
        html += f"""
                        <div style="margin: 8px 0; padding: 8px; background-color: #fff; border-left: 3px solid #f39c12; border-radius: 3px;">
                            <code style="font-weight: bold; color: #2c3e50;">{col_schema.name}</code>
                            <div style="margin-top: 5px; font-size: 0.9em;">
                                Physical: <code>{col_schema.physical_type}</code>
        """

        if col_schema.logical_type:
            html += f" | Logical: <code>{col_schema.logical_type}</code>"

        html += f"""
                                <br>Precision: {col_schema.precision}, Scale: {col_schema.scale}, Length: {col_schema.length}
                                <br>Max Definition Level: {col_schema.max_definition_level},
                                Max Repetition Level: {col_schema.max_repetition_level}
                                <br>Path in Schema: {col_schema.path}
                            </div>
                        </div>
        """

    html += """
                    </div>
                </details>
            </div>
        </div>
    """

    # 4. FOOTER
    footer_start = file_size - 8
    html += f"""
        <div style="margin-bottom: 25px;">
            <h4 style="color: #16a085; margin: 10px 0;">
                4️⃣ Footer
            </h4>
            <div style="margin-left: 20px; background-color: #f0f4ff; padding: 15px; border-radius: 5px; border: 1px solid #b8c5e8;">
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold; width: 180px;" colspan="2">
                            <strong>Metadata Length (4 bytes)</strong>
                        </td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold; width: 180px;">Location:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">Bytes {footer_start:,} - {footer_start + 3:,}</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">Value:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;"><code>{metadata_len}</code> bytes</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;" colspan="2">
                            <strong>Magic Number (4 bytes)</strong>
                        </td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">Location:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">Bytes {file_size - 4:,} - {file_size - 1:,}</td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">Content:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;"><code style="background-color: #e8e8e8; padding: 2px 6px; border-radius: 3px;">PAR1</code></td>
                    </tr>
                    <tr style="background-color: #fff;">
                        <td style="padding: 5px; border: 1px solid #ddd; font-weight: bold;">Purpose:</td>
                        <td style="padding: 5px; border: 1px solid #ddd;">End-of-file marker and format verification</td>
                    </tr>
                </table>
            </div>
        </div>
    """

    display(HTML(html))

def calculate_sizes(parquet_file: pq.ParquetFile):
    metadata = parquet_file.metadata
    sizes = {parquet_file.schema[i].name: {'dict': 0, 'data': 0} for i in range(metadata.num_columns)}
    for i in range(metadata.num_row_groups):
      rg = metadata.row_group(i)
      for col_idx in range(rg.num_columns):
        col = rg.column(col_idx)
        col_name = col.path_in_schema
        sizes[col_name]['dict'] += col.data_page_offset - col.dictionary_page_offset if col.has_dictionary_page else 0
        sizes[col_name]['data'] += col.total_compressed_size
    return sizes

def compare_sizes(p1: pq.ParquetFile, role1: str, p2: pq.ParquetFile, role2: str):
    size1 = calculate_sizes(p1)
    size2 = calculate_sizes(p2)
    all_columns = sorted(list(set(size1.keys()) | set(size2.keys())))

    comparison_table = []
    for col in all_columns:
        s1 = size1.get(col, {'dict': 0, 'data': 0})
        s2 = size2.get(col, {'dict': 0, 'data': 0})

        comparison_table.append({
            'Column': col,
            f'{role1} Dict Size': s1['dict'],
            f'{role2} Dict Size': s2['dict'],
            'Dict Size %': f"{(s2['dict'] / s1['dict'] * 100):.2f}%" if s1['dict'] > 0 else 'N/A',
            f'{role1} Data Size': s1['data'],
            f'{role2} Data Size': s2['data'],
            'Data Size %': f"{(s2['data'] / s1['data'] * 100):.2f}%" if s1['data'] > 0 else 'N/A',
        })
    df_comparison = pd.DataFrame(comparison_table)
    display(df_comparison)
