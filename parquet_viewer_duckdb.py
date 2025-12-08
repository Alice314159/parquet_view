#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
from pathlib import Path

import duckdb
from PyQt6.QtCore import Qt, QSettings, QTimer, pyqtSignal
from PyQt6.QtGui import (
    QColor, QFont, QDragEnterEvent, QDropEvent,
    QIcon, QGuiApplication, QCursor, QFontMetrics, QIntValidator,
    QFileOpenEvent,
)
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
    QTableWidgetItem, QPushButton, QLineEdit, QLabel, QSplitter, QTreeWidget,
    QTreeWidgetItem, QHeaderView, QMessageBox, QFileDialog, QTabWidget,
    QStyledItemDelegate, QMenu
)

# =====================================================================
# 小工具
# =====================================================================

def resource_path(relative: str) -> str:
    """兼容 PyInstaller onefile 资源定位"""
    if hasattr(sys, "_MEIPASS"):
        return str(Path(sys._MEIPASS) / relative)
    return str(Path.cwd() / relative)


def get_base_font() -> QFont:
    """根据平台返回一个合适的基础字体（整体偏大一点，适配 MacBook）"""
    if sys.platform == "darwin":
        # Mac
        return QFont("PingFang SC", 13)
    elif sys.platform.startswith("win"):
        return QFont("Microsoft YaHei UI", 10)
    else:
        # Linux / 其他
        return QFont("Noto Sans CJK SC", 10)

# =====================================================================
# 让编辑框更清晰的委托
# =====================================================================

class StrongEditorDelegate(QStyledItemDelegate):
    """为 QTableWidget 提供更醒目的编辑器（白底、深色字、粗蓝边框、进入时全选）"""

    def createEditor(self, parent, option, index):
        editor = QLineEdit(parent)
        editor.setFont(get_base_font())
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

# =====================================================================
# 单个 Parquet 标签页
# =====================================================================

class ParquetTab(QWidget):
    """单个 Parquet 文件标签页（DuckDB 版本，支持排序、分页、CSV 导出）"""

    def __init__(self, file_path: str | None = None):
        super().__init__()

        self.file_path: str | None = None
        self.con: duckdb.DuckDBPyConnection | None = None
        self.table_cache = None
        self.columns: list[str] = []
        self.current_sql = "SELECT * FROM t LIMIT 100"

        # 分页相关
        self.page_size = 100
        self.current_page = 1
        self.total_rows = 0
        self.total_pages = 1
        self.base_sql = "SELECT * FROM t"
        self.prev_btn: QPushButton | None = None
        self.next_btn: QPushButton | None = None
        self.page_input: QLineEdit | None = None
        self.page_size_input: QLineEdit | None = None

        # 排序相关
        self.sort_column: str | None = None
        self.sort_order = Qt.SortOrder.AscendingOrder
        self.base_header_labels: list[str] = []

        # Qt 控件占位
        self.file_info_label: QLabel | None = None
        self.tree_widget: QTreeWidget | None = None
        self.sql_input: QLineEdit | None = None
        self.status_label: QLabel | None = None
        self.table_widget: QTableWidget | None = None

        self.init_ui()
        if file_path:
            self.load_file(file_path)

    # ------------------------------------------------------------------
    # UI 构建
    # ------------------------------------------------------------------

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

    def create_left_panel(self) -> QWidget:
        left = QWidget()
        left.setMaximumWidth(280)
        left.setObjectName("leftPanel")
        v = QVBoxLayout(left)
        v.setContentsMargins(0, 0, 0, 0)
        v.setSpacing(0)

        base_font = get_base_font()

        # 顶部标题 + 文件信息卡片
        title_widget = QWidget()
        title_widget.setObjectName("titleWidget")
        tlay = QVBoxLayout(title_widget)
        tlay.setContentsMargins(15, 15, 15, 15)
        tlay.setSpacing(8)

        title_label = QLabel("文件结构")
        title_font = QFont(base_font.family(), base_font.pointSize() + 2, QFont.Weight.Bold)
        title_label.setFont(title_font)
        tlay.addWidget(title_label)

        info_card = QWidget()
        info_card.setObjectName("infoCard")
        iclay = QVBoxLayout(info_card)
        iclay.setContentsMargins(10, 10, 10, 10)
        iclay.setSpacing(4)

        self.file_info_label = QLabel("未加载文件")
        info_font = QFont(base_font.family(), base_font.pointSize())
        self.file_info_label.setFont(info_font)
        self.file_info_label.setWordWrap(True)
        self.file_info_label.setStyleSheet("color: #6b7280;")
        iclay.addWidget(self.file_info_label)

        tlay.addWidget(info_card)
        v.addWidget(title_widget)

        # 列信息树
        self.tree_widget = QTreeWidget()
        item_font = QFont(base_font.family(), base_font.pointSize() + 1)
        header_font = QFont(base_font.family(), base_font.pointSize() + 1, QFont.Weight.DemiBold)

        self.tree_widget.setFont(item_font)
        self.tree_widget.setHeaderLabels(["名称 (Name)", "类型 (Type)"])

        header_item = self.tree_widget.headerItem()
        if header_item is not None:
            header_item.setFont(0, header_font)
            header_item.setFont(1, header_font)

        self.tree_widget.setColumnWidth(0, 150)
        self.tree_widget.setIndentation(15)

        v.addWidget(self.tree_widget)
        return left

    def create_right_panel(self) -> QWidget:
        base_font = get_base_font()
        right = QWidget()
        right.setObjectName("rightPanel")
        v = QVBoxLayout(right)
        v.setContentsMargins(0, 0, 0, 0)
        v.setSpacing(0)

        # 顶部工具栏
        toolbar = QWidget()
        toolbar.setObjectName("toolbar")
        hlay = QHBoxLayout(toolbar)
        hlay.setContentsMargins(20, 12, 20, 12)
        hlay.setSpacing(12)

        btn_font = QFont(base_font.family(), base_font.pointSize())
        btn_style = (
            "padding: 7px 18px; font-size: 10pt; border-radius: 6px; "
            "color: #ffffff; border: none;"
        )

        add_btn = QPushButton("➕ 新增行")
        add_btn.setFont(btn_font)
        add_btn.setStyleSheet(btn_style + "background-color: #3b82f6;")
        add_btn.clicked.connect(self.add_row)

        del_btn = QPushButton("🗑 删除选中")
        del_btn.setFont(btn_font)
        del_btn.setStyleSheet(btn_style + "background-color: #ef4444;")
        del_btn.clicked.connect(self.delete_selected)

        reset_btn = QPushButton("🔄 重置视图")
        reset_btn.setFont(btn_font)
        reset_btn.setStyleSheet(btn_style + "background-color: #6b7280;")
        reset_btn.clicked.connect(self.reset_view)

        export_csv_btn = QPushButton("📥 导出 CSV")
        export_csv_btn.setFont(btn_font)
        export_csv_btn.setStyleSheet(btn_style + "background-color: #8b5cf6;")
        csv_menu = QMenu(self)
        csv_menu.addAction("导出当前页", self.export_current_page_csv)
        csv_menu.addAction("导出全部数据", self.export_all_csv)
        export_csv_btn.setMenu(csv_menu)

        save_btn = QPushButton("💾 保存为 Parquet")
        save_btn.setFont(btn_font)
        save_btn.setStyleSheet(btn_style + "background-color: #059669;")
        save_btn.clicked.connect(self.save_file)

        for b in (add_btn, del_btn, reset_btn, export_csv_btn, save_btn):
            hlay.addWidget(b)
        hlay.addStretch()
        v.addWidget(toolbar)

        # 内容区域
        content = QWidget()
        content.setObjectName("contentWidget")
        c = QVBoxLayout(content)
        c.setContentsMargins(20, 10, 20, 20)
        c.setSpacing(10)

        sql_label = QLabel("SQL:")
        sql_label.setFont(QFont(base_font.family(), base_font.pointSize(), QFont.Weight.Bold))
        sql_label.setStyleSheet("color: #374151;")
        c.addWidget(sql_label)

        sql_line = QHBoxLayout()
        sql_line.setSpacing(10)

        self.sql_input = QLineEdit()
        self.sql_input.setPlaceholderText(
            "输入 SQL 查询... (例如: SELECT * FROM t WHERE open < 100 ORDER BY trade_date DESC)"
        )
        self.sql_input.setText("SELECT * FROM t LIMIT 100")
        self.sql_input.setMinimumHeight(38)
        self.sql_input.setFont(QFont(base_font.family(), base_font.pointSize() + 1))
        self.sql_input.returnPressed.connect(self.run_query)
        sql_line.addWidget(self.sql_input)

        run_btn = QPushButton("▶ 运行")
        run_btn.setMinimumWidth(90)
        run_btn.setMinimumHeight(38)
        run_btn.setFont(btn_font)
        run_btn.setStyleSheet(
            "font-size: 10pt; padding: 0 24px; font-weight: 600; "
            "border-radius: 6px; background-color: #3b82f6; color: white;"
        )
        run_btn.clicked.connect(self.run_query)
        sql_line.addWidget(run_btn)
        c.addLayout(sql_line)

        self.status_label = QLabel("状态: 就绪")
        self.status_label.setFont(QFont(base_font.family(), base_font.pointSize() - 1))
        self.status_label.setStyleSheet("color: #6b7280; padding: 3px 0;")
        c.addWidget(self.status_label)

        # 分页工具条
        pager_line = QHBoxLayout()
        pager_line.setSpacing(10)

        size_label = QLabel("每页行数:")
        size_label.setFont(QFont(base_font.family(), base_font.pointSize() - 1))
        size_label.setStyleSheet("color: #6b7280;")
        pager_line.addWidget(size_label)

        self.page_size_input = QLineEdit()
        self.page_size_input.setFixedWidth(70)
        self.page_size_input.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.page_size_input.setFont(QFont(base_font.family(), base_font.pointSize()))
        self.page_size_input.setValidator(QIntValidator(1, 100000, self))
        self.page_size_input.setText(str(self.page_size))
        self.page_size_input.returnPressed.connect(self.on_page_size_changed)
        pager_line.addWidget(self.page_size_input)

        pager_line.addStretch()

        self.prev_btn = QPushButton("⟨")
        self.prev_btn.setFixedSize(32, 26)
        self.prev_btn.setFont(QFont(base_font.family(), base_font.pointSize()))
        self.prev_btn.clicked.connect(self.prev_page)
        pager_line.addWidget(self.prev_btn)

        self.page_input = QLineEdit()
        self.page_input.setPlaceholderText("页")
        self.page_input.setFixedWidth(80)
        self.page_input.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.page_input.setFont(QFont(base_font.family(), base_font.pointSize()))
        self.page_input.returnPressed.connect(self.goto_page)
        pager_line.addWidget(self.page_input)

        self.next_btn = QPushButton("⟩")
        self.next_btn.setFixedSize(32, 26)
        self.next_btn.setFont(QFont(base_font.family(), base_font.pointSize()))
        self.next_btn.clicked.connect(self.next_page)
        pager_line.addWidget(self.next_btn)

        c.addLayout(pager_line)

        # 数据表
        self.table_widget = QTableWidget()
        table_font = QFont(base_font.family(), base_font.pointSize() + 1)
        self.table_widget.setFont(table_font)
        self.table_widget.setAlternatingRowColors(True)
        self.table_widget.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table_widget.verticalHeader().setDefaultSectionSize(36)
        self.table_widget.verticalHeader().setMinimumSectionSize(30)
        self.table_widget.setEditTriggers(
            QTableWidget.EditTrigger.DoubleClicked
            | QTableWidget.EditTrigger.EditKeyPressed
            | QTableWidget.EditTrigger.AnyKeyPressed
        )
        self.table_widget.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.Interactive
        )
        self.table_widget.horizontalHeader().setStretchLastSection(True)
        self.table_widget.verticalHeader().setVisible(True)
        self.table_widget.verticalHeader().setDefaultAlignment(
            Qt.AlignmentFlag.AlignCenter
        )

        self.table_widget.setSortingEnabled(False)
        self.table_widget.horizontalHeader().sectionClicked.connect(
            self.on_header_clicked
        )
        self.table_widget.horizontalHeader().setSortIndicatorShown(False)
        self.table_widget.setItemDelegate(StrongEditorDelegate(self.table_widget))

        c.addWidget(self.table_widget)
        v.addWidget(content)
        return right

    # ------------------------------------------------------------------
    # DuckDB / 数据加载
    # ------------------------------------------------------------------

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

            self.con.execute("DROP VIEW IF EXISTS t;")
            norm_path = file_path.replace("\\", "/")
            self.con.execute(
                f"CREATE VIEW t AS SELECT * FROM parquet_scan('{norm_path}');"
            )

            meta = self.con.execute("SELECT * FROM t LIMIT 1")
            self.columns = [desc[0] for desc in meta.description] if meta.description else []

            size_mb = os.path.getsize(file_path) / 1024 / 1024
            self.base_sql = "SELECT * FROM t"
            self.page_size = 100
            self.current_page = 1
            self.total_rows = self.con.execute("SELECT COUNT(*) FROM t").fetchone()[0]

            file_name = os.path.basename(file_path)
            self.file_info_label.setText(
                f"文件: {file_name}\n"
                f"大小: {size_mb:.2f} MB\n"
                f"行数: {self.total_rows}\n"
                f"列数: {len(self.columns)}"
            )

            if self.page_size_input:
                self.page_size_input.setText(str(self.page_size))

            self.update_tree()
            self.sql_input.setText("SELECT * FROM t LIMIT 100")
            self._refresh_current_page()
            self.status_label.setText("状态: 加载成功")
            return True
        except Exception as e:
            QMessageBox.critical(self, "错误", f"无法打开文件:\n{e}")
            return False

    def update_tree(self):
        self.tree_widget.clear()
        if not self.con:
            return

        base_font = get_base_font()
        root = QTreeWidgetItem(self.tree_widget)
        root.setText(0, "数据表")
        root.setFont(0, QFont(base_font.family(), base_font.pointSize(), QFont.Weight.Bold))

        columns_node = QTreeWidgetItem(root)
        columns_node.setText(0, "列 (Columns)")
        columns_node.setFont(0, QFont(base_font.family(), base_font.pointSize(), QFont.Weight.Bold))

        try:
            desc = self.con.execute("DESCRIBE t").fetchall()
            for name, col_type, *_ in desc:
                item = QTreeWidgetItem(columns_node)
                item.setText(0, name)
                item.setText(1, col_type)
        except Exception as e:
            item = QTreeWidgetItem(columns_node)
            item.setText(0, "无法获取列信息")
            item.setText(1, str(e))

        self.tree_widget.expandAll()

    # ------------------------------------------------------------------
    # 分页 / 查询
    # ------------------------------------------------------------------

    def _update_pager_display(self):
        self.total_pages = max(1, (self.total_rows + self.page_size - 1) // self.page_size)
        if self.current_page > self.total_pages:
            self.current_page = self.total_pages

        if self.page_input:
            self.page_input.setText(f"{self.current_page}/{self.total_pages}")

        if self.prev_btn:
            self.prev_btn.setEnabled(self.current_page > 1)
        if self.next_btn:
            self.next_btn.setEnabled(self.current_page < self.total_pages)

    def _recount_total_rows(self):
        try:
            count_sql = f"SELECT COUNT(*) FROM ({self.base_sql}) sub"
            self.total_rows = self.con.execute(count_sql).fetchone()[0]
        except Exception:
            self.total_rows = self.table_widget.rowCount()

    def _prepare_base_sql_from_input(self):
        text = self.sql_input.text().strip().rstrip(";")
        if not text:
            text = "SELECT * FROM t"

        tokens = text.split()
        uppers = [t.upper() for t in tokens]
        page_size = self.page_size

        if "LIMIT" in uppers:
            idx = uppers.index("LIMIT")
            if idx + 1 < len(tokens):
                try:
                    page_size = int(tokens[idx + 1])
                except ValueError:
                    pass
            tokens = tokens[:idx]
            text = " ".join(tokens)

        self.base_sql = text.strip() or "SELECT * FROM t"
        self.page_size = max(1, page_size)
        if self.page_size_input:
            self.page_size_input.setText(str(self.page_size))

    def _refresh_current_page(self):
        if not self.con:
            return
        offset = (self.current_page - 1) * self.page_size
        page_sql = f"SELECT * FROM ({self.base_sql}) sub LIMIT {self.page_size} OFFSET {offset}"
        self.current_sql = page_sql
        self.run_sql_to_table(page_sql)
        self._update_pager_display()
        self.status_label.setText(f"状态: 第 {self.current_page} 页查询成功")

    def on_page_size_changed(self):
        if not self.page_size_input:
            return
        text = self.page_size_input.text().strip()
        if not text:
            return
        try:
            new_size = int(text)
            if new_size <= 0:
                raise ValueError
        except ValueError:
            QMessageBox.information(self, "提示", "请输入大于 0 的整数作为每页行数。")
            self.page_size_input.setText(str(self.page_size))
            return

        self.page_size = new_size
        base = self.base_sql.strip() or "SELECT * FROM t"
        self.sql_input.setText(f"{base} LIMIT {self.page_size}")
        try:
            self._recount_total_rows()
            self.current_page = 1
            self._refresh_current_page()
        except Exception as e:
            QMessageBox.warning(self, "错误", f"更新每页行数失败:\n{e}")
            self.page_size_input.setText(str(self.page_size))

    def run_query(self):
        if not self.con:
            QMessageBox.warning(self, "警告", "没有数据可查询")
            return
        self._prepare_base_sql_from_input()
        try:
            self._recount_total_rows()
            self.current_page = 1
            self._refresh_current_page()
        except Exception as e:
            QMessageBox.warning(self, "查询错误", f"SQL 查询失败:\n{e}")

    def prev_page(self):
        if not self.con or self.total_pages <= 1:
            return
        if self.current_page > 1:
            self.current_page -= 1
            self._refresh_current_page()

    def next_page(self):
        if not self.con or self.total_pages <= 1:
            return
        if self.current_page < self.total_pages:
            self.current_page += 1
            self._refresh_current_page()

    def goto_page(self):
        if not self.con or self.total_pages <= 1:
            return
        text = self.page_input.text().strip()
        if not text:
            return
        if "/" in text:
            text = text.split("/", 1)[0].strip()
        try:
            page = int(text)
        except ValueError:
            QMessageBox.information(self, "提示", "请输入正确的页码（正整数）。")
            self._update_pager_display()
            return

        if page < 1:
            page = 1
        if page > self.total_pages:
            page = self.total_pages
        self.current_page = page
        self._refresh_current_page()

    # ------------------------------------------------------------------
    # SQL 执行 & 显示
    # ------------------------------------------------------------------

    def run_sql_to_table(self, sql: str):
        res = self.con.execute(sql)
        self.columns = [desc[0] for desc in res.description] if res.description else []
        rows = [dict(zip(self.columns, row)) for row in res.fetchall()]
        self.table_cache = rows
        self.display_data(self.columns, rows)

    def display_data(self, columns, rows):
        self.table_widget.clear()
        self.table_widget.setColumnCount(len(columns))
        self.table_widget.setHorizontalHeaderLabels(columns)
        self.table_widget.setRowCount(len(rows))

        self.base_header_labels = list(columns)
        self._update_header_sort_icons(sorted_index=None)

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

                if col.lower() in ("change", "change_rate", "pct", "pct_chg"):
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

        font = self.table_widget.font()
        fm = QFontMetrics(font)
        header_font = self.table_widget.horizontalHeader().font()
        header_fm = QFontMetrics(header_font)

        for c in range(self.table_widget.columnCount()):
            header_text = self.table_widget.horizontalHeaderItem(c).text()
            header_width = header_fm.horizontalAdvance(header_text) + 30

            max_content_width = 0
            sample_rows = min(80, self.table_widget.rowCount())
            for r in range(sample_rows):
                item = self.table_widget.item(r, c)
                if item and item.text():
                    text = item.text()
                    text_width = fm.horizontalAdvance(text) + 30
                    max_content_width = max(max_content_width, text_width)

            optimal_width = max(header_width, max_content_width)
            MIN_WIDTH = 110
            MAX_WIDTH = 420

            if any(
                kw in header_text.lower()
                for kw in ["desc", "note", "comment", "remark", "描述", "备注", "说明"]
            ):
                MAX_WIDTH = 600

            if any(kw in header_text.lower() for kw in ["id", "code", "代码", "编号"]):
                MIN_WIDTH = 90
                MAX_WIDTH = 220

            final_width = max(MIN_WIDTH, min(optimal_width, MAX_WIDTH))
            self.table_widget.setColumnWidth(c, int(final_width))

        total_width = sum(
            self.table_widget.columnWidth(c) for c in range(self.table_widget.columnCount())
        )
        available_width = self.table_widget.viewport().width()

        if total_width < available_width and self.table_widget.columnCount() > 0:
            extra_space = available_width - total_width
            cols_to_expand = min(3, self.table_widget.columnCount())
            extra_per_col = extra_space // cols_to_expand

            for i in range(cols_to_expand):
                c = self.table_widget.columnCount() - 1 - i
                current_width = self.table_widget.columnWidth(c)
                new_width = min(current_width + extra_per_col, 600)
                self.table_widget.setColumnWidth(c, new_width)

    # ------------------------------------------------------------------
    # 列头排序 + 小三角
    # ------------------------------------------------------------------

    def _update_header_sort_icons(self, sorted_index: int | None):
        if self.table_widget.columnCount() == 0:
            return

        if not self.base_header_labels or len(self.base_header_labels) != self.table_widget.columnCount():
            self.base_header_labels = []
            for i in range(self.table_widget.columnCount()):
                item = self.table_widget.horizontalHeaderItem(i)
                self.base_header_labels.append(item.text() if item else f"col_{i}")

        for i in range(self.table_widget.columnCount()):
            item = self.table_widget.horizontalHeaderItem(i)
            if not item:
                continue
            base = self.base_header_labels[i] if i < len(self.base_header_labels) else item.text()

            if sorted_index is not None and i == sorted_index and self.sort_column is not None:
                arrow = "▲" if self.sort_order == Qt.SortOrder.AscendingOrder else "▼"
                item.setText(f"{base} {arrow}")
            else:
                item.setText(base)

    def on_header_clicked(self, logical_index: int):
        if not self.con or not self.columns:
            return

        col_name = self.columns[logical_index]

        if self.sort_column == col_name:
            self.sort_order = (
                Qt.SortOrder.DescendingOrder
                if self.sort_order == Qt.SortOrder.AscendingOrder
                else Qt.SortOrder.AscendingOrder
            )
        else:
            self.sort_column = col_name
            self.sort_order = Qt.SortOrder.AscendingOrder

        order_dir = "ASC" if self.sort_order == Qt.SortOrder.AscendingOrder else "DESC"
        escaped_col = col_name.replace('"', '""')

        raw_sql = self.sql_input.text().strip()
        if not raw_sql:
            raw_sql = "SELECT * FROM t LIMIT 100"
        upper = raw_sql.upper()

        limit_clause = ""
        limit_pos = upper.rfind(" LIMIT ")
        if limit_pos != -1:
            limit_clause = raw_sql[limit_pos:].strip()
            base_sql = raw_sql[:limit_pos].strip()
        else:
            base_sql = raw_sql
            limit_clause = f"LIMIT {self.page_size}"

        upper_base = base_sql.upper()
        order_pos = upper_base.rfind(" ORDER BY ")
        if order_pos != -1:
            base_sql = base_sql[:order_pos].strip()

        if " FROM " not in upper_base:
            base_sql = "SELECT * FROM t"

        sort_sql = f'{base_sql} ORDER BY "{escaped_col}" {order_dir} {limit_clause}'.strip()

        try:
            self.run_sql_to_table(sort_sql)
            self.sql_input.setText(sort_sql)
            arrow = "▲" if self.sort_order == Qt.SortOrder.AscendingOrder else "▼"
            self.status_label.setText(
                f"状态: 按 {col_name} {arrow} 排序，当前页 {self.table_widget.rowCount()} 行"
            )
            self._update_header_sort_icons(sorted_index=logical_index)
        except Exception as e:
            QMessageBox.warning(self, "排序错误", f"排序失败:\n{e}")

    # ------------------------------------------------------------------
    # CSV 导出
    # ------------------------------------------------------------------

    def export_current_page_csv(self):
        if self.table_widget.columnCount() == 0:
            QMessageBox.information(self, "提示", "没有数据可导出。")
            return

        default_dir = os.path.dirname(self.file_path) if self.file_path else ""
        default_name = (os.path.splitext(os.path.basename(self.file_path))[0] + "_page.csv") if self.file_path else "current_page.csv"
        file_path, _ = QFileDialog.getSaveFileName(
            self, "导出当前页为 CSV",
            os.path.join(default_dir, default_name),
            "CSV Files (*.csv)"
        )
        if not file_path:
            return

        try:
            cols = [
                self.table_widget.horizontalHeaderItem(i).text().replace(" ▲", "").replace(" ▼", "")
                for i in range(self.table_widget.columnCount())
            ]
            data = []
            for r in range(self.table_widget.rowCount()):
                row = []
                for c in range(self.table_widget.columnCount()):
                    it = self.table_widget.item(r, c)
                    s = it.text() if it else ""
                    row.append(s)
                data.append(row)

            self._ensure_con()
            self.con.execute("DROP TABLE IF EXISTS __tmp_csv__;")
            cols_ddl = ", ".join(f'"{name}" VARCHAR' for name in cols)
            self.con.execute(f"CREATE TABLE __tmp_csv__ ({cols_ddl});")
            if data:
                placeholders = ", ".join(["?"] * len(cols))
                self.con.executemany(
                    f"INSERT INTO __tmp_csv__ VALUES ({placeholders})", data
                )

            norm_path = file_path.replace("\\", "/")
            self.con.execute(
                f"COPY __tmp_csv__ TO '{norm_path}' (HEADER, DELIMITER ',');"
            )
            QMessageBox.information(
                self, "成功", f"当前页数据已导出！\n共 {len(data)} 行"
            )
            self.status_label.setText(
                f"状态: 已导出当前页到 {os.path.basename(file_path)}"
            )
        except Exception as e:
            QMessageBox.critical(self, "错误", f"导出失败:\n{e}")

    def export_all_csv(self):
        if not self.con or not self.file_path:
            QMessageBox.information(self, "提示", "没有加载文件，无法导出全部数据。")
            return

        default_dir = os.path.dirname(self.file_path)
        default_name = os.path.splitext(os.path.basename(self.file_path))[0] + "_all.csv"
        file_path, _ = QFileDialog.getSaveFileName(
            self, "导出全部数据为 CSV",
            os.path.join(default_dir, default_name),
            "CSV Files (*.csv)"
        )
        if not file_path:
            return

        try:
            total_rows = self.con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
            norm_path = file_path.replace("\\", "/")
            self.con.execute(
                f"COPY t TO '{norm_path}' (HEADER, DELIMITER ',');"
            )
            QMessageBox.information(
                self, "成功", f"全部数据已导出！\n共 {total_rows} 行"
            )
            self.status_label.setText(
                f"状态: 已导出全部数据到 {os.path.basename(file_path)}"
            )
        except Exception as e:
            QMessageBox.critical(self, "错误", f"导出失败:\n{e}")

    # ------------------------------------------------------------------
    # 表格编辑 & 保存
    # ------------------------------------------------------------------

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
        self.table_widget.scrollToItem(
            self.table_widget.item(r, 0),
            QTableWidget.ScrollHint.PositionAtBottom,
        )
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
            self.base_sql = "SELECT * FROM t"
            self.page_size = 100
            self.current_page = 1
            if self.page_size_input:
                self.page_size_input.setText(str(self.page_size))
            self._recount_total_rows()
            self.sql_input.setText("SELECT * FROM t LIMIT 100")
            self._refresh_current_page()
            self._update_header_sort_icons(sorted_index=None)
        except Exception as e:
            QMessageBox.warning(self, "错误", f"重置失败: {e}")

    def save_file(self):
        if not self.con:
            QMessageBox.information(self, "提示", "没有数据可保存。")
            return

        if self.file_path:
            default_dir = os.path.dirname(self.file_path)
            default_name = os.path.basename(self.file_path)
        else:
            default_dir = ""
            default_name = "data.parquet"

        file_path, _ = QFileDialog.getSaveFileName(
            self, "保存为 Parquet 文件",
            os.path.join(default_dir, default_name),
            "Parquet Files (*.parquet)"
        )
        if not file_path:
            return

        try:
            norm_path = file_path.replace("\\", "/")
            self.con.execute(
                f"COPY (SELECT * FROM t) TO '{norm_path}' (FORMAT PARQUET);"
            )
            QMessageBox.information(self, "成功", "文件保存成功！")
            self.status_label.setText(f"状态: 已保存到 {os.path.basename(file_path)}")
            self.file_path = file_path
        except Exception as e:
            QMessageBox.critical(self, "错误", f"保存失败:\n{e}")

# =====================================================================
# 主窗口
# =====================================================================

class ParquetViewer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.settings = QSettings("ParquetViewer", "Settings")
        self.recent_files: list[str] = []
        self.load_settings()

        self.tab_widget: QTabWidget | None = None
        self.recent_menu: QMenu | None = None

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
        base_font = get_base_font()

        self.setWindowTitle("Parquet 文件查看器 (DuckDB) - 增强版")
        self.setGeometry(100, 100, 1400, 840)

        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        toolbar = QWidget()
        toolbar.setObjectName("mainToolbar")
        toolbar_layout = QHBoxLayout(toolbar)
        toolbar_layout.setContentsMargins(20, 10, 20, 10)
        toolbar_layout.setSpacing(12)

        big_btn_style = "padding: 8px 20px; font-size: 11pt; font-weight: 600; border-radius: 6px;"

        open_btn = QPushButton("📂 打开文件")
        open_btn.setFont(QFont(base_font.family(), base_font.pointSize() + 1))
        open_btn.setStyleSheet(big_btn_style + "background-color: #3b82f6; color: white; border: none;")
        open_btn.clicked.connect(self.open_file)
        toolbar_layout.addWidget(open_btn)

        recent_btn = QPushButton("🕘 最近打开")
        recent_btn.setFont(QFont(base_font.family(), base_font.pointSize()))
        recent_btn.setStyleSheet(big_btn_style + "background-color: #6b7280; color: white; border: none;")
        self.recent_menu = QMenu(self)
        recent_btn.setMenu(self.recent_menu)
        toolbar_layout.addWidget(recent_btn)
        self.refresh_recent_menu()

        new_tab_btn = QPushButton("➕ 新建标签")
        new_tab_btn.setFont(QFont(base_font.family(), base_font.pointSize()))
        new_tab_btn.setStyleSheet(big_btn_style + "background-color: #10b981; color: white; border: none;")
        new_tab_btn.clicked.connect(self.new_tab)
        toolbar_layout.addWidget(new_tab_btn)

        toolbar_layout.addStretch()

        close_tab_btn = QPushButton("✖ 关闭当前标签")
        close_tab_btn.setFont(QFont(base_font.family(), base_font.pointSize()))
        close_tab_btn.setStyleSheet(big_btn_style + "background-color: #ef4444; color: white; border: none;")
        close_tab_btn.clicked.connect(self.close_current_tab)
        toolbar_layout.addWidget(close_tab_btn)

        main_layout.addWidget(toolbar)

        self.tab_widget = QTabWidget()
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.setMovable(True)
        self.tab_widget.tabCloseRequested.connect(self.close_tab)
        main_layout.addWidget(self.tab_widget)

        # 注意：这里不再 self.new_tab()，
        # 是否创建默认标签由 main() 里的 QTimer 决定

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
            QLineEdit {
                background-color: #ffffff;
                border: 1px solid #d1d5db;
                border-radius: 6px;
                padding: 8px 12px;
                color: #111827;
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
            QHeaderView::up-arrow, QHeaderView::down-arrow {
                width: 0px;
                height: 0px;
            }
            QTreeWidget {
                background-color: #ffffff;
                border: none;
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

    # 最近文件
    def load_settings(self):
        recent = self.settings.value("recent_files", [])
        if isinstance(recent, str):
            recent = [recent]
        self.recent_files = list(recent) if recent else []

    def save_settings(self):
        self.settings.setValue("recent_files", self.recent_files[:10])

    def add_recent_file(self, file_path: str):
        file_path = os.path.abspath(file_path)
        if file_path in self.recent_files:
            self.recent_files.remove(file_path)
        self.recent_files.insert(0, file_path)
        self.recent_files = self.recent_files[:10]
        self.save_settings()
        self.refresh_recent_menu()

    def refresh_recent_menu(self):
        if not self.recent_menu:
            return
        self.recent_menu.clear()
        if not self.recent_files:
            act = self.recent_menu.addAction("（无）")
            act.setEnabled(False)
            return
        for path in self.recent_files:
            act = self.recent_menu.addAction(path)
            act.triggered.connect(lambda _, p=path: self.open_file_in_new_tab(p))

    # 标签页管理
    def new_tab(self):
        tab = ParquetTab()
        idx = self.tab_widget.addTab(tab, "新标签")
        self.tab_widget.setCurrentIndex(idx)

    def open_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "打开 Parquet 文件", "",
            "Parquet Files (*.parquet);;All Files (*)"
        )
        if file_path:
            self.open_file_in_new_tab(file_path)

    def open_file_in_new_tab(self, file_path: str):
        if not file_path:
            return
        abs_path = os.path.abspath(file_path)

        # 如果已经打开过该文件，则直接切换
        for i in range(self.tab_widget.count()):
            widget = self.tab_widget.widget(i)
            if isinstance(widget, ParquetTab):
                opened_path = getattr(widget, "file_path", None)
                if opened_path and os.path.abspath(opened_path) == abs_path:
                    self.tab_widget.setCurrentIndex(i)
                    return

        tab = ParquetTab(abs_path)
        file_name = os.path.basename(abs_path)
        idx = self.tab_widget.addTab(tab, file_name)
        self.tab_widget.setCurrentIndex(idx)
        self.add_recent_file(abs_path)

    def close_tab(self, index: int):
        if self.tab_widget.count() > 1:
            self.tab_widget.removeTab(index)

    def close_current_tab(self):
        idx = self.tab_widget.currentIndex()
        if idx >= 0 and self.tab_widget.count() > 1:
            self.tab_widget.removeTab(idx)

    # 拖拽打开
    def dragEnterEvent(self, event: QDragEnterEvent):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dropEvent(self, event: QDropEvent):
        for url in event.mimeData().urls():
            file_path = url.toLocalFile()
            if file_path.lower().endswith(".parquet"):
                self.open_file_in_new_tab(file_path)

# =====================================================================
# 自定义 QApplication
# =====================================================================

class ParquetApplication(QApplication):
    fileOpened = pyqtSignal(str)

    def event(self, event):
        if isinstance(event, QFileOpenEvent):
            file_path = event.file()
            if file_path:
                self.fileOpened.emit(file_path)
            return True
        return super().event(event)

# =====================================================================
# main
# =====================================================================

def main():
    app = ParquetApplication(sys.argv)
    app.setStyle("Fusion")
    app.setFont(QFont("Microsoft YaHei UI", 11))

    viewer = ParquetViewer()

    # 记录是否已经通过命令行 / Finder 打开过文件
    file_opened_flag = {"opened": False}

    def handle_file_open(path: str):
        file_opened_flag["opened"] = True
        viewer.open_file_in_new_tab(path)

    app.fileOpened.connect(handle_file_open)

    # 命令行参数（打包后通过 "app file.parquet" 打开时会走这里）
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            if arg.startswith("-"):
                continue
            if arg.lower().endswith(".parquet") and os.path.exists(arg):
                file_opened_flag["opened"] = True
                viewer.open_file_in_new_tab(arg)
                break

    # 延迟 800ms，如果这时还没有任何文件被打开，就自动创建一个“新标签”
    def ensure_default_tab():
        if (not file_opened_flag["opened"]) and viewer.tab_widget.count() == 0:
            viewer.new_tab()

    QTimer.singleShot(800, ensure_default_tab)

    viewer.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
