#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
from pathlib import Path

import duckdb
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
    QTableWidgetItem, QPushButton, QLineEdit, QLabel, QSplitter, QTreeWidget,
    QTreeWidgetItem, QHeaderView, QMessageBox, QFileDialog, QTabWidget,
    QStyledItemDelegate, QMenu
)
from PyQt6.QtCore import Qt, QSettings, QTimer
from PyQt6.QtGui import QColor, QFont, QDragEnterEvent, QDropEvent, QIcon, QGuiApplication, QCursor, QFontMetrics


def resource_path(relative: str) -> str:
    """兼容 PyInstaller onefile 资源定位"""
    if hasattr(sys, "_MEIPASS"):
        return str(Path(sys._MEIPASS) / relative)
    return str(Path.cwd() / relative)


# ========================== 让编辑框更清晰的委托 ==========================
class StrongEditorDelegate(QStyledItemDelegate):
    """为 QTableWidget 提供更醒目的编辑器（白底、深色字、粗蓝边框、进入时全选）"""

    def createEditor(self, parent, option, index):
        editor = QLineEdit(parent)
        editor.setFont(QFont("Microsoft YaHei UI", 10))
        editor.setStyleSheet("""
            QLineEdit {
                background: #ffffff;
                color: #111827;
                border: 2px solid #2563eb;
                border-radius: 6px;
                padding: 4px 6px;
                selection-background-color: #2563eb;
                selection-color: #ffffff;
            }
        """)
        editor.setFrame(True)
        return editor

    def setEditorData(self, editor, index):
        super().setEditorData(editor, index)
        editor.selectAll()


# =======================================================================


class ParquetTab(QWidget):
    """单个 Parquet 文件标签页（DuckDB 版本，支持排序和CSV导出）"""

    def __init__(self, file_path=None):
        super().__init__()
        self.file_path = None
        self.con: duckdb.DuckDBPyConnection | None = None
        self.table_cache = None
        self.columns = []
        self.current_sql = "SELECT * FROM t LIMIT 100"  # 记录当前SQL
        self.sort_column = None  # 当前排序列
        self.sort_order = Qt.SortOrder.AscendingOrder  # 当前排序方向
        self.init_ui()
        if file_path:
            self.load_file(file_path)

    def init_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        left_panel = self.create_left_panel()
        splitter.addWidget(left_panel)

        right_panel = self.create_right_panel()
        splitter.addWidget(right_panel)

        splitter.setSizes([280, 1120])
        layout.addWidget(splitter)

    def create_left_panel(self):
        left = QWidget()
        left.setMaximumWidth(280)
        left.setObjectName("leftPanel")
        v = QVBoxLayout(left)
        v.setContentsMargins(0, 0, 0, 0)
        v.setSpacing(0)

        title_widget = QWidget()
        title_widget.setObjectName("titleWidget")
        tlay = QVBoxLayout(title_widget)
        tlay.setContentsMargins(15, 15, 15, 15)

        title_label = QLabel("文件结构")
        title_label.setFont(QFont("Microsoft YaHei UI", 11, QFont.Weight.Bold))
        tlay.addWidget(title_label)

        info_card = QWidget()
        info_card.setObjectName("infoCard")
        iclay = QVBoxLayout(info_card)
        iclay.setContentsMargins(10, 10, 10, 10)

        self.file_info_label = QLabel("未加载文件")
        self.file_info_label.setFont(QFont("Microsoft YaHei UI", 8))
        self.file_info_label.setWordWrap(True)
        self.file_info_label.setStyleSheet("color: #6b7280;")
        iclay.addWidget(self.file_info_label)

        tlay.addWidget(info_card)
        v.addWidget(title_widget)

        self.tree_widget = QTreeWidget()
        self.tree_widget.setHeaderLabels(["名称 (Name)", "类型 (Type)"])
        self.tree_widget.setColumnWidth(0, 150)
        self.tree_widget.setIndentation(15)
        v.addWidget(self.tree_widget)
        return left

    def create_right_panel(self):
        right = QWidget()
        right.setObjectName("rightPanel")
        v = QVBoxLayout(right)
        v.setContentsMargins(0, 0, 0, 0)
        v.setSpacing(0)

        # 顶部工具栏
        toolbar = QWidget()
        toolbar.setObjectName("toolbar")
        hlay = QHBoxLayout(toolbar)
        hlay.setContentsMargins(20, 15, 20, 15)

        file_info_layout = QHBoxLayout()
        file_icon = QLabel("📄")
        file_icon.setFont(QFont("Segoe UI Emoji", 12))
        file_info_layout.addWidget(file_icon)

        self.file_label = QLabel("未打开文件")
        self.file_label.setFont(QFont("Microsoft YaHei UI", 10))
        self.file_label.setStyleSheet("color: #374151;")
        file_info_layout.addWidget(self.file_label)
        file_info_layout.addStretch()
        hlay.addLayout(file_info_layout)
        hlay.addStretch()

        btn_style = "padding: 7px 16px; font-size: 9pt;"

        add_btn = QPushButton("➕ 新增行")
        add_btn.setStyleSheet(btn_style)
        add_btn.clicked.connect(self.add_row)
        del_btn = QPushButton("🗑️ 删除选中")
        del_btn.setStyleSheet(btn_style)
        del_btn.clicked.connect(self.delete_selected)
        reset_btn = QPushButton("🔄 重置视图")
        reset_btn.setStyleSheet(btn_style)
        reset_btn.clicked.connect(self.reset_view)

        # CSV 导出按钮（带下拉菜单）
        export_csv_btn = QPushButton("📥 导出 CSV")
        export_csv_btn.setStyleSheet(btn_style + "background-color: #8b5cf6;")
        csv_menu = QMenu(self)
        csv_menu.addAction("导出当前页", self.export_current_page_csv)
        csv_menu.addAction("导出全部数据", self.export_all_csv)
        export_csv_btn.setMenu(csv_menu)

        save_btn = QPushButton("💾 保存为 Parquet")
        save_btn.setStyleSheet(btn_style + "background-color: #059669;")
        save_btn.clicked.connect(self.save_file)

        for b in (add_btn, del_btn, reset_btn, export_csv_btn, save_btn):
            hlay.addWidget(b)

        v.addWidget(toolbar)

        # 内容区域
        content = QWidget()
        content.setObjectName("contentWidget")
        c = QVBoxLayout(content)
        c.setContentsMargins(20, 15, 20, 20)
        c.setSpacing(12)

        sql_label = QLabel("SQL:")
        sql_label.setFont(QFont("Microsoft YaHei UI", 9, QFont.Weight.Bold))
        sql_label.setStyleSheet("color: #374151;")
        c.addWidget(sql_label)

        sql_line = QHBoxLayout()
        sql_line.setSpacing(10)

        self.sql_input = QLineEdit()
        self.sql_input.setPlaceholderText("输入 SQL 查询... (例如: SELECT * FROM t ORDER BY column_name LIMIT 100)")
        self.sql_input.setText("SELECT * FROM t LIMIT 100")
        self.sql_input.setMinimumHeight(38)
        self.sql_input.returnPressed.connect(self.run_query)
        sql_line.addWidget(self.sql_input)

        run_btn = QPushButton("▶ 运行")
        run_btn.setMinimumWidth(90)
        run_btn.setMinimumHeight(38)
        run_btn.setStyleSheet("font-size: 9pt; padding: 0 24px; font-weight: 600;")
        run_btn.clicked.connect(self.run_query)
        sql_line.addWidget(run_btn)

        c.addLayout(sql_line)

        self.status_label = QLabel("状态: 就绪")
        self.status_label.setFont(QFont("Microsoft YaHei UI", 8))
        self.status_label.setStyleSheet("color: #6b7280; padding: 5px 0;")
        c.addWidget(self.status_label)

        self.table_widget = QTableWidget()
        self.table_widget.setAlternatingRowColors(True)
        self.table_widget.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table_widget.verticalHeader().setDefaultSectionSize(36)
        self.table_widget.verticalHeader().setMinimumSectionSize(36)
        self.table_widget.setFont(QFont("Microsoft YaHei UI", 9))
        self.table_widget.setEditTriggers(QTableWidget.EditTrigger.DoubleClicked |
                                          QTableWidget.EditTrigger.EditKeyPressed |
                                          QTableWidget.EditTrigger.AnyKeyPressed)
        self.table_widget.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table_widget.horizontalHeader().setStretchLastSection(True)
        self.table_widget.verticalHeader().setVisible(True)
        self.table_widget.verticalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)

        # 启用排序功能
        self.table_widget.setSortingEnabled(False)  # 禁用默认排序，使用自定义排序
        self.table_widget.horizontalHeader().sectionClicked.connect(self.on_header_clicked)

        # 安装"编辑态强化"委托
        self.table_widget.setItemDelegate(StrongEditorDelegate(self.table_widget))

        c.addWidget(self.table_widget)
        v.addWidget(content)
        return right

    # ---------- 列头点击排序 ----------
    def on_header_clicked(self, logical_index):
        """点击列头进行排序"""
        if not self.con or not self.columns:
            return

        col_name = self.columns[logical_index]

        # 切换排序方向
        if self.sort_column == col_name:
            self.sort_order = Qt.SortOrder.DescendingOrder if self.sort_order == Qt.SortOrder.AscendingOrder else Qt.SortOrder.AscendingOrder
        else:
            self.sort_column = col_name
            self.sort_order = Qt.SortOrder.AscendingOrder

        # 构建带排序的 SQL
        order_dir = "ASC" if self.sort_order == Qt.SortOrder.AscendingOrder else "DESC"
        escaped_col = col_name.replace('"', '""')

        # 从当前 SQL 中提取 LIMIT 子句
        current_sql = self.sql_input.text().strip().upper()
        limit_clause = ""
        if "LIMIT" in current_sql:
            parts = self.sql_input.text().strip().split()
            for i, part in enumerate(parts):
                if part.upper() == "LIMIT" and i + 1 < len(parts):
                    limit_clause = f" LIMIT {parts[i + 1]}"
                    break

        if not limit_clause:
            limit_clause = " LIMIT 100"

        # 构建新的 SQL（移除原有的 ORDER BY）
        base_sql = "SELECT * FROM t"
        sort_sql = f'{base_sql} ORDER BY "{escaped_col}" {order_dir}{limit_clause}'

        try:
            self.run_sql_to_table(sort_sql)
            self.sql_input.setText(sort_sql)
            arrow = "↑" if self.sort_order == Qt.SortOrder.AscendingOrder else "↓"
            self.status_label.setText(f"状态: 按 {col_name} {arrow} 排序，共 {self.table_widget.rowCount()} 行")
        except Exception as e:
            QMessageBox.warning(self, "排序错误", f"排序失败:\n{e}")

    # ---------- CSV 导出功能 ----------
    def export_current_page_csv(self):
        """导出当前页面显示的数据为 CSV"""
        if self.table_widget.columnCount() == 0:
            QMessageBox.information(self, "提示", "没有数据可导出。")
            return

        default_dir = os.path.dirname(self.file_path) if self.file_path else ""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "导出当前页为 CSV", default_dir, "CSV Files (*.csv)"
        )
        if not file_path:
            return

        try:
            # 收集当前表格数据
            cols = [self.table_widget.horizontalHeaderItem(i).text() for i in range(self.table_widget.columnCount())]
            data = []
            for r in range(self.table_widget.rowCount()):
                row = []
                for c in range(self.table_widget.columnCount()):
                    it = self.table_widget.item(r, c)
                    s = it.text() if it else ""
                    row.append(s)
                data.append(row)

            # 写入临时表并导出
            self._ensure_con()
            self.con.execute("DROP TABLE IF EXISTS __tmp_csv__;")
            cols_ddl = ", ".join(f'"{name}" VARCHAR' for name in cols)
            self.con.execute(f"CREATE TABLE __tmp_csv__ ({cols_ddl});")
            if data:
                placeholders = ", ".join(["?"] * len(cols))
                self.con.executemany(f'INSERT INTO __tmp_csv__ VALUES ({placeholders})', data)

            self.con.execute(f"COPY __tmp_csv__ TO '{file_path.replace('\\', '/')}' (HEADER, DELIMITER ',');")
            QMessageBox.information(self, "成功", f"当前页数据已导出！\n共 {len(data)} 行")
            self.status_label.setText(f"状态: 已导出当前页到 {os.path.basename(file_path)}")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"导出失败:\n{e}")

    def export_all_csv(self):
        """导出全部数据为 CSV（从原始 parquet 文件）"""
        if not self.con or not self.file_path:
            QMessageBox.information(self, "提示", "没有加载文件，无法导出全部数据。")
            return

        default_dir = os.path.dirname(self.file_path) if self.file_path else ""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "导出全部数据为 CSV", default_dir, "CSV Files (*.csv)"
        )
        if not file_path:
            return

        try:
            # 统计总行数
            total_rows = self.con.execute("SELECT COUNT(*) FROM t").fetchone()[0]

            # 直接从 VIEW t 导出全部数据
            self.con.execute(f"COPY t TO '{file_path.replace('\\', '/')}' (HEADER, DELIMITER ',');")

            QMessageBox.information(self, "成功", f"全部数据已导出！\n共 {total_rows} 行")
            self.status_label.setText(f"状态: 已导出全部数据到 {os.path.basename(file_path)}")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"导出失败:\n{e}")

    # ---------- 数据读写 ----------
    def _ensure_con(self):
        if self.con is None:
            self.con = duckdb.connect()
            self.con.execute("PRAGMA threads=4;")

    def load_file(self, file_path: str) -> bool:
        try:
            file_path = os.path.abspath(file_path)
            if not os.path.exists(file_path):
                raise FileNotFoundError(file_path)

            self._ensure_con()
            self.file_path = file_path

            # 以 VIEW 形式映射 parquet
            self.con.execute("DROP VIEW IF EXISTS t;")
            self.con.execute(f"CREATE VIEW t AS SELECT * FROM parquet_scan('{file_path.replace('\\', '/')}');")

            # 读取 schema
            meta = self.con.execute("SELECT * FROM t LIMIT 1")
            self.columns = [desc[0] for desc in meta.description] if meta.description else []

            # 文件信息
            file_name = os.path.basename(file_path)
            self.file_label.setText(file_name)
            size_mb = os.path.getsize(file_path) / 1024 / 1024
            total_rows = self.con.execute("SELECT COUNT(*) FROM t").fetchone()[0]

            self.file_info_label.setText(
                f"文件: {file_name}\n大小: {size_mb:.2f} MB\n行数: {total_rows}\n列数: {len(self.columns)}"
            )

            self.update_tree()
            self.run_sql_to_table("SELECT * FROM t LIMIT 100")
            self.status_label.setText(f"状态: 成功加载，预览 100 行（总 {total_rows}）")
            return True
        except Exception as e:
            QMessageBox.critical(self, "错误", f"无法打开文件:\n{e}")
            return False

    def update_tree(self):
        """使用 DuckDB 扫描当前 parquet，推断列名和类型，并更新文件结构树。"""
        self.tree_widget.clear()

        if not getattr(self, "file_path", None):
            return

        if not getattr(self, "con", None):
            self.con = duckdb.connect()

        root = QTreeWidgetItem(self.tree_widget)
        root.setText(0, "数据表")
        root.setFont(0, QFont("Microsoft YaHei UI", 9, QFont.Weight.Bold))

        columns_node = QTreeWidgetItem(root)
        columns_node.setText(0, "列 (Columns)")
        columns_node.setFont(0, QFont("Microsoft YaHei UI", 9, QFont.Weight.Bold))

        try:
            rel = self.con.execute("SELECT * FROM t LIMIT 0")
            desc = rel.description
            col_names = [d[0] for d in desc]

            for name in col_names:
                escaped = name.replace('"', '""')
                ident = f'"{escaped}"'

                sql = f"SELECT typeof({ident}) FROM t WHERE {ident} IS NOT NULL LIMIT 1"

                try:
                    res = self.con.execute(sql).fetchone()
                    if res and res[0] is not None:
                        col_type = res[0]
                    else:
                        col_type = "UNKNOWN"
                except Exception:
                    try:
                        res = self.con.execute(f"SELECT typeof({ident}) FROM t LIMIT 1").fetchone()
                        col_type = res[0] if res and res[0] else "UNKNOWN"
                    except Exception:
                        col_type = "ERROR"

                item = QTreeWidgetItem(columns_node)
                item.setText(0, name)
                item.setText(1, col_type)

        except Exception as e:
            item = QTreeWidgetItem(columns_node)
            item.setText(0, "无法获取列信息")
            item.setText(1, str(e))

        self.tree_widget.expandAll()

    def run_sql_to_table(self, sql: str):
        res = self.con.execute(sql)
        self.columns = [desc[0] for desc in res.description] if res.description else []
        rows = [dict(zip(self.columns, row)) for row in res.fetchall()]

        self.table_cache = rows
        self.current_sql = sql
        self.display_data(self.columns, rows)

    def display_data(self, columns, rows):
        """显示数据并智能设置列宽"""
        self.table_widget.clear()
        self.table_widget.setColumnCount(len(columns))
        self.table_widget.setHorizontalHeaderLabels(columns)
        self.table_widget.setRowCount(len(rows))

        for i, row in enumerate(rows):
            for j, col in enumerate(columns):
                val = row.get(col, None)
                if val is None:
                    s = ""
                elif isinstance(val, float):
                    s = f"{val:.6g}"
                else:
                    s = str(val)
                item = QTableWidgetItem(s)
                if col in ("change", "change_rate", "pct", "pct_chg"):
                    try:
                        fv = float(s)
                        if fv > 0:
                            item.setForeground(QColor(220, 38, 38))
                        elif fv < 0:
                            item.setForeground(QColor(22, 163, 74))
                    except Exception:
                        pass
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table_widget.setItem(i, j, item)

        # ========== 智能列宽设置 ==========
        # 获取字体度量
        font = self.table_widget.font()
        fm = QFontMetrics(font)
        header_font = self.table_widget.horizontalHeader().font()
        header_fm = QFontMetrics(header_font)

        # 为每列计算最佳宽度
        for c in range(self.table_widget.columnCount()):
            # 1. 计算列标题宽度
            header_text = self.table_widget.horizontalHeaderItem(c).text()
            header_width = header_fm.horizontalAdvance(header_text) + 30  # 加边距

            # 2. 计算内容最大宽度（采样前50行以提高性能）
            max_content_width = 0
            sample_rows = min(50, self.table_widget.rowCount())

            for r in range(sample_rows):
                item = self.table_widget.item(r, c)
                if item and item.text():
                    text = item.text()
                    # 计算文本宽度
                    text_width = fm.horizontalAdvance(text) + 30  # 加边距和图标空间
                    max_content_width = max(max_content_width, text_width)

            # 3. 取标题和内容宽度的较大值
            optimal_width = max(header_width, max_content_width)

            # 4. 应用合理的最小值和最大值限制
            MIN_WIDTH = 100  # 最小宽度
            MAX_WIDTH = 400  # 最大宽度（防止过宽）

            # 特殊处理：超长文本列（如描述、备注等）可以更宽
            if any(keyword in header_text.lower() for keyword in
                   ['desc', 'note', 'comment', 'remark', '描述', '备注', '说明']):
                MAX_WIDTH = 600

            # 特殊处理：ID、代码等固定格式列可以更窄
            if any(keyword in header_text.lower() for keyword in ['id', 'code', '代码', '编号']):
                MIN_WIDTH = 80
                MAX_WIDTH = 200

            # 应用宽度限制
            final_width = max(MIN_WIDTH, min(optimal_width, MAX_WIDTH))

            self.table_widget.setColumnWidth(c, int(final_width))

        # 5. 如果总宽度小于表格宽度，适当拉伸最后几列
        total_width = sum(self.table_widget.columnWidth(c) for c in range(self.table_widget.columnCount()))
        available_width = self.table_widget.viewport().width()

        if total_width < available_width and self.table_widget.columnCount() > 0:
            # 将剩余空间分配给最后几列（最多3列）
            extra_space = available_width - total_width
            cols_to_expand = min(3, self.table_widget.columnCount())
            extra_per_col = extra_space // cols_to_expand

            for i in range(cols_to_expand):
                c = self.table_widget.columnCount() - 1 - i
                current_width = self.table_widget.columnWidth(c)
                new_width = min(current_width + extra_per_col, 600)  # 不超过600
                self.table_widget.setColumnWidth(c, new_width)

    # ---------- 交互 ----------
    def run_query(self):
        if not self.con:
            QMessageBox.warning(self, "警告", "没有数据可查询")
            return
        q = self.sql_input.text().strip()
        if not q:
            return
        try:
            self.run_sql_to_table(q)
            self.status_label.setText(f"状态: 查询成功，共 {self.table_widget.rowCount()} 行")
        except Exception as e:
            QMessageBox.warning(self, "查询错误", f"SQL 查询失败:\n{e}")

    def add_row(self):
        cols = self.table_widget.columnCount()
        if cols == 0:
            QMessageBox.information(self, "提示", "当前没有列，无法新增行。")
            return
        r = self.table_widget.rowCount()
        self.table_widget.insertRow(r)
        for c in range(cols):
            it = QTableWidgetItem("")
            it.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            it.setBackground(QColor(255, 255, 255))
            self.table_widget.setItem(r, c, it)
        self.table_widget.scrollToItem(self.table_widget.item(r, 0), QTableWidget.ScrollHint.PositionAtBottom)
        self.table_widget.selectRow(r)
        self.table_widget.setCurrentCell(r, 0)
        self.table_widget.resizeRowToContents(r)
        self.status_label.setText(f"状态: 已添加新行 (第 {r + 1} 行)")

    def delete_selected(self):
        rows = sorted({it.row() for it in self.table_widget.selectedItems()}, reverse=True)
        if not rows:
            QMessageBox.information(self, "提示", "请先选择要删除的行")
            return
        for r in rows:
            self.table_widget.removeRow(r)
        self.status_label.setText(f"状态: 已删除 {len(rows)} 行")

    def reset_view(self):
        if not self.con:
            return
        try:
            self.sort_column = None
            self.sort_order = Qt.SortOrder.AscendingOrder
            self.sql_input.setText("SELECT * FROM t LIMIT 100")
            self.run_sql_to_table("SELECT * FROM t LIMIT 100")
            self.status_label.setText("状态: 已重置为前 100 行")
        except Exception as e:
            QMessageBox.warning(self, "错误", f"重置失败: {e}")

    def _gather_table_to_duckdb(self, tmp_table_name="__tmp_edit__"):
        """把当前 QTableWidget 的内容灌到 DuckDB 临时表，用于导出 parquet。"""
        cols = [self.table_widget.horizontalHeaderItem(i).text() for i in range(self.table_widget.columnCount())]
        data = []
        for r in range(self.table_widget.rowCount()):
            row = []
            for c in range(self.table_widget.columnCount()):
                it = self.table_widget.item(r, c)
                s = it.text() if it else ""
                row.append(s)
            data.append(row)

        self._ensure_con()
        self.con.execute(f"DROP TABLE IF EXISTS {tmp_table_name};")
        cols_ddl = ", ".join(f'"{name}" VARCHAR' for name in cols)
        self.con.execute(f"CREATE TABLE {tmp_table_name} ({cols_ddl});")
        if data:
            placeholders = ", ".join(["?"] * len(cols))
            self.con.executemany(
                f'INSERT INTO {tmp_table_name} VALUES ({placeholders})',
                data
            )
        return cols

    def save_file(self):
        if self.table_widget.columnCount() == 0:
            QMessageBox.information(self, "提示", "没有数据可保存。")
            return

        default_dir = os.path.dirname(self.file_path) if self.file_path else ""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "保存为 Parquet 文件", default_dir, "Parquet Files (*.parquet)"
        )
        if not file_path:
            return
        try:
            _ = self._gather_table_to_duckdb()
            self.con.execute(f"COPY __tmp_edit__ TO '{file_path.replace('\\', '/')}' (FORMAT PARQUET);")
            QMessageBox.information(self, "成功", "文件保存成功！")
            self.status_label.setText(f"状态: 已保存到 {os.path.basename(file_path)}")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"保存失败:\n{e}")


class ParquetViewer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.settings = QSettings("ParquetViewer", "Settings")
        self.recent_files = []
        self.load_settings()
        self.init_ui()
        self.apply_stylesheet()

        ico = resource_path("app.ico")
        if os.path.exists(ico):
            self.setWindowIcon(QIcon(ico))

        self.setAcceptDrops(True)
        QTimer.singleShot(100, self.center_on_active_screen)

    def center_on_active_screen(self):
        screen = QGuiApplication.screenAt(QCursor.pos())
        if screen is None and self.windowHandle() is not None:
            screen = self.windowHandle().screen()
        if screen is None:
            screen = QGuiApplication.primaryScreen()
        if not screen:
            return
        avail = screen.availableGeometry()
        geo = self.frameGeometry()
        geo.moveCenter(avail.center())
        self.move(geo.topLeft())

    def init_ui(self):
        self.setWindowTitle('Parquet 文件查看器 (DuckDB) - 增强版')
        self.setGeometry(100, 100, 1400, 820)

        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        toolbar = QWidget()
        toolbar.setObjectName("mainToolbar")
        toolbar_layout = QHBoxLayout(toolbar)
        toolbar_layout.setContentsMargins(20, 12, 20, 12)

        open_btn = QPushButton("📂 打开文件")
        open_btn.setStyleSheet("padding: 8px 20px; font-size: 10pt; font-weight: 600;")
        open_btn.clicked.connect(self.open_file)
        toolbar_layout.addWidget(open_btn)

        new_tab_btn = QPushButton("➕ 新建标签")
        new_tab_btn.setStyleSheet("padding: 8px 20px; font-size: 10pt;")
        new_tab_btn.clicked.connect(self.new_tab)
        toolbar_layout.addWidget(new_tab_btn)

        toolbar_layout.addStretch()

        close_tab_btn = QPushButton("✖ 关闭当前标签")
        close_tab_btn.setStyleSheet("padding: 8px 20px; font-size: 10pt;")
        close_tab_btn.clicked.connect(self.close_current_tab)
        toolbar_layout.addWidget(close_tab_btn)

        main_layout.addWidget(toolbar)

        self.tab_widget = QTabWidget()
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.setMovable(True)
        self.tab_widget.tabCloseRequested.connect(self.close_tab)
        main_layout.addWidget(self.tab_widget)

        # 添加初始标签
        self.new_tab()

    def apply_stylesheet(self):
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f9fafb;
            }
            QWidget#mainToolbar {
                background-color: #ffffff;
                border-bottom: 1px solid #e5e7eb;
            }
            QWidget#leftPanel {
                background-color: #f3f4f6;
                border-right: 1px solid #e5e7eb;
            }
            QWidget#titleWidget {
                background-color: #ffffff;
                border-bottom: 1px solid #e5e7eb;
            }
            QWidget#infoCard {
                background-color: #f9fafb;
                border: 1px solid #e5e7eb;
                border-radius: 6px;
            }
            QWidget#toolbar {
                background-color: #ffffff;
                border-bottom: 1px solid #e5e7eb;
            }
            QWidget#contentWidget {
                background-color: #ffffff;
            }
            QPushButton {
                background-color: #3b82f6;
                color: white;
                border: none;
                border-radius: 6px;
                font-weight: 500;
            }
            QPushButton:hover {
                background-color: #2563eb;
            }
            QPushButton:pressed {
                background-color: #1d4ed8;
            }
            QLineEdit {
                background-color: #ffffff;
                border: 1px solid #d1d5db;
                border-radius: 6px;
                padding: 8px 12px;
                color: #111827;
                font-size: 9pt;
            }
            QLineEdit:focus {
                border: 2px solid #3b82f6;
                padding: 7px 11px;
            }
            QTableWidget {
                background-color: #ffffff;
                border: 1px solid #e5e7eb;
                border-radius: 8px;
                gridline-color: #f3f4f6;
            }
            QTableWidget::item {
                padding: 5px;
                border-bottom: 1px solid #f3f4f6;
            }
            QTableWidget::item:selected {
                background-color: #dbeafe;
                color: #1e40af;
            }
            QHeaderView::section {
                background-color: #f9fafb;
                color: #374151;
                padding: 8px;
                border: none;
                border-bottom: 2px solid #e5e7eb;
                border-right: 1px solid #e5e7eb;
                font-weight: 600;
            }
            QHeaderView::section:hover {
                background-color: #f3f4f6;
            }
            QTreeWidget {
                background-color: #ffffff;
                border: none;
                font-size: 9pt;
            }
            QTreeWidget::item {
                padding: 5px;
                border-bottom: 1px solid #f3f4f6;
            }
            QTreeWidget::item:selected {
                background-color: #dbeafe;
                color: #1e40af;
            }
            QTreeWidget::item:hover {
                background-color: #f3f4f6;
            }
            QTabWidget::pane {
                border: none;
                background-color: #ffffff;
            }
            QTabBar::tab {
                background-color: #f3f4f6;
                color: #6b7280;
                padding: 10px 20px;
                margin-right: 2px;
                border-top-left-radius: 6px;
                border-top-right-radius: 6px;
            }
            QTabBar::tab:selected {
                background-color: #ffffff;
                color: #111827;
                font-weight: 600;
            }
            QTabBar::tab:hover {
                background-color: #e5e7eb;
            }
            QMenu {
                background-color: #ffffff;
                border: 1px solid #e5e7eb;
                border-radius: 6px;
                padding: 5px;
            }
            QMenu::item {
                padding: 8px 20px;
                border-radius: 4px;
            }
            QMenu::item:selected {
                background-color: #f3f4f6;
            }
        """)

    def load_settings(self):
        recent = self.settings.value("recent_files", [])
        if isinstance(recent, str):
            recent = [recent]
        self.recent_files = recent if recent else []

    def save_settings(self):
        self.settings.setValue("recent_files", self.recent_files[:10])

    def add_recent_file(self, file_path):
        if file_path in self.recent_files:
            self.recent_files.remove(file_path)
        self.recent_files.insert(0, file_path)
        self.save_settings()

    def new_tab(self):
        tab = ParquetTab()
        idx = self.tab_widget.addTab(tab, "新标签")
        self.tab_widget.setCurrentIndex(idx)

    def open_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "打开 Parquet 文件", "", "Parquet Files (*.parquet);;All Files (*)"
        )
        if file_path:
            self.open_file_in_new_tab(file_path)

    def open_file_in_new_tab(self, file_path):
        tab = ParquetTab(file_path)
        file_name = os.path.basename(file_path)
        idx = self.tab_widget.addTab(tab, file_name)
        self.tab_widget.setCurrentIndex(idx)
        self.add_recent_file(file_path)

    def close_tab(self, index):
        if self.tab_widget.count() > 1:
            self.tab_widget.removeTab(index)

    def close_current_tab(self):
        idx = self.tab_widget.currentIndex()
        if idx >= 0 and self.tab_widget.count() > 1:
            self.tab_widget.removeTab(idx)

    def dragEnterEvent(self, event: QDragEnterEvent):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dropEvent(self, event: QDropEvent):
        for url in event.mimeData().urls():
            file_path = url.toLocalFile()
            if file_path.lower().endswith('.parquet'):
                self.open_file_in_new_tab(file_path)


def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')

    # 设置应用字体
    app.setFont(QFont("Microsoft YaHei UI", 9))

    viewer = ParquetViewer()
    viewer.show()

    sys.exit(app.exec())


if __name__ == '__main__':
    main()