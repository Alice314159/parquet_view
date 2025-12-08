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
from PyQt6.QtGui import (
    QColor, QFont, QDragEnterEvent, QDropEvent,
    QIcon, QGuiApplication, QCursor, QFontMetrics
)


def resource_path(relative: str) -> str:
    """兼容 PyInstaller onefile 资源定位"""
    if hasattr(sys, "_MEIPASS"):
        return str(Path(sys._MEIPASS) / relative)
    return str(Path.cwd() / relative)


def get_base_font_family() -> str:
    """根据平台选择一个比较合适的中文/英文字体"""
    if sys.platform == "darwin":
        return "PingFang SC"  # macOS
    elif sys.platform.startswith("win"):
        return "Microsoft YaHei UI"
    else:
        return "Microsoft YaHei UI"


def get_base_font_size() -> int:
    """基础字号（mac 再大一点）"""
    if sys.platform == "darwin":
        return 13
    else:
        return 10


# ========================== 编辑框委托 ==========================
class StrongEditorDelegate(QStyledItemDelegate):
    """为 QTableWidget 提供更醒目的编辑器（白底、深色字、粗蓝边框、进入时全选）"""
    def createEditor(self, parent, option, index):
        editor = QLineEdit(parent)
        editor.setFont(QFont(get_base_font_family(), get_base_font_size()))
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
    """单个 Parquet 文件标签页（DuckDB 版本，支持排序、CSV 导出、分页）"""
    def __init__(self, file_path=None):
        super().__init__()
        self.file_path: str | None = None
        self.con: duckdb.DuckDBPyConnection | None = None
        self.table_cache = None
        self.columns: list[str] = []
        self.current_sql = "SELECT * FROM t LIMIT 100"

        # ====== 分页相关 ======
        self.page_size = 100
        self.current_page = 1
        self.total_rows = 0
        self.total_pages = 1
        self.base_sql = "SELECT * FROM t"
        self.page_info_label: QLabel | None = None
        self.prev_btn: QPushButton | None = None
        self.next_btn: QPushButton | None = None
        self.page_input: QLineEdit | None = None
        # ======================

        self.sort_column = None
        self.sort_order = Qt.SortOrder.AscendingOrder

        self.init_ui()
        if file_path:
            self.load_file(file_path)

    # ------------------------------------------------------------------ UI

    def init_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        left_panel = self.create_left_panel()
        splitter.addWidget(left_panel)

        right_panel = self.create_right_panel()
        splitter.addWidget(right_panel)

        splitter.setSizes([260, 1140])
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)

        layout.addWidget(splitter)

    def create_left_panel(self):
        left = QWidget()
        left.setMaximumWidth(260)
        left.setObjectName("leftPanel")
        v = QVBoxLayout(left)
        v.setContentsMargins(0, 0, 0, 0)
        v.setSpacing(0)

        base_size = get_base_font_size()

        title_widget = QWidget()
        title_widget.setObjectName("titleWidget")
        tlay = QVBoxLayout(title_widget)
        tlay.setContentsMargins(12, 10, 12, 8)

        title_label = QLabel("文件结构")
        title_label.setFont(QFont(get_base_font_family(), base_size + 1, QFont.Weight.Bold))
        tlay.addWidget(title_label)

        info_card = QWidget()
        info_card.setObjectName("infoCard")
        iclay = QVBoxLayout(info_card)
        iclay.setContentsMargins(8, 8, 8, 8)

        self.file_info_label = QLabel("未加载文件")
        self.file_info_label.setFont(QFont(get_base_font_family(), base_size))
        self.file_info_label.setWordWrap(True)
        self.file_info_label.setStyleSheet("color: #6b7280;")
        iclay.addWidget(self.file_info_label)

        tlay.addWidget(info_card)
        v.addWidget(title_widget)

        self.tree_widget = QTreeWidget()
        self.tree_widget.setHeaderLabels(["名称 (Name)", "类型 (Type)"])
        self.tree_widget.setColumnWidth(0, 150)
        self.tree_widget.setIndentation(16)
        self.tree_widget.setFont(QFont(get_base_font_family(), base_size + 1))
        header = self.tree_widget.header()
        header.setFont(QFont(get_base_font_family(), base_size + 1, QFont.Weight.Medium))
        v.addWidget(self.tree_widget)
        return left

    def create_right_panel(self):
        right = QWidget()
        right.setObjectName("rightPanel")
        v = QVBoxLayout(right)
        v.setContentsMargins(0, 0, 0, 0)
        v.setSpacing(0)

        base_size = get_base_font_size()
        table_font_size = base_size + 2  # 表格字号再大一点

        # 顶部工具栏
        toolbar = QWidget()
        toolbar.setObjectName("toolbar")
        hlay = QHBoxLayout(toolbar)
        hlay.setContentsMargins(16, 8, 16, 8)
        hlay.setSpacing(10)

        btn_style = "padding: 6px 14px; font-size: 10pt;"

        add_btn = QPushButton("➕ 新增行")
        add_btn.setStyleSheet(btn_style)
        add_btn.clicked.connect(self.add_row)

        del_btn = QPushButton("🗑️ 删除选中")
        del_btn.setStyleSheet(btn_style)
        del_btn.clicked.connect(self.delete_selected)

        reset_btn = QPushButton("🔄 重置视图")
        reset_btn.setStyleSheet(btn_style)
        reset_btn.clicked.connect(self.reset_view)

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

        hlay.addStretch()
        v.addWidget(toolbar)

        # 内容区域
        content = QWidget()
        content.setObjectName("contentWidget")
        c = QVBoxLayout(content)
        c.setContentsMargins(16, 10, 16, 12)
        c.setSpacing(8)

        sql_label = QLabel("SQL:")
        sql_label.setFont(QFont(get_base_font_family(), base_size + 1, QFont.Weight.Bold))
        sql_label.setStyleSheet("color: #374151;")
        c.addWidget(sql_label)

        sql_line = QHBoxLayout()
        sql_line.setSpacing(8)

        self.sql_input = QLineEdit()
        self.sql_input.setPlaceholderText(
            "输入 SQL 查询... (例如: SELECT * FROM t WHERE open < 100 ORDER BY trade_date DESC)"
        )
        self.sql_input.setText("SELECT * FROM t LIMIT 100")
        self.sql_input.setMinimumHeight(34)
        self.sql_input.setFont(QFont(get_base_font_family(), base_size + 1))
        self.sql_input.returnPressed.connect(self.run_query)
        sql_line.addWidget(self.sql_input)

        run_btn = QPushButton("▶ 运行")
        run_btn.setMinimumWidth(80)
        run_btn.setMinimumHeight(34)
        run_btn.setStyleSheet("font-size: 10pt; padding: 0 18px; font-weight: 600;")
        run_btn.clicked.connect(self.run_query)
        sql_line.addWidget(run_btn)

        c.addLayout(sql_line)

        self.status_label = QLabel("状态: 就绪")
        self.status_label.setFont(QFont(get_base_font_family(), base_size))
        self.status_label.setStyleSheet("color: #6b7280; padding: 3px 0;")
        c.addWidget(self.status_label)

        # ===== 分页工具条 =====
        pager_line = QHBoxLayout()
        pager_line.setSpacing(6)

        spacer = QLabel("")
        spacer.setFont(QFont(get_base_font_family(), base_size))
        spacer.setStyleSheet("color: #6b7280;")
        pager_line.addWidget(spacer)
        pager_line.addStretch()

        self.prev_btn = QPushButton("⟨")
        self.prev_btn.setFixedSize(30, 24)
        self.prev_btn.clicked.connect(self.prev_page)
        pager_line.addWidget(self.prev_btn)

        self.page_input = QLineEdit()
        self.page_input.setPlaceholderText("页")
        self.page_input.setFixedWidth(80)
        self.page_input.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.page_input.setFont(QFont(get_base_font_family(), base_size))
        self.page_input.returnPressed.connect(self.goto_page)
        pager_line.addWidget(self.page_input)

        self.next_btn = QPushButton("⟩")
        self.next_btn.setFixedSize(30, 24)
        self.next_btn.clicked.connect(self.next_page)
        pager_line.addWidget(self.next_btn)

        c.addLayout(pager_line)

        # ======================== 表格 ========================
        self.table_widget = QTableWidget()
        self.table_widget.setAlternatingRowColors(True)
        self.table_widget.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table_widget.verticalHeader().setDefaultSectionSize(32)
        self.table_widget.verticalHeader().setMinimumSectionSize(28)
        self.table_widget.setFont(QFont(get_base_font_family(), table_font_size))

        header_font = QFont(get_base_font_family(), table_font_size, QFont.Weight.Medium)
        self.table_widget.horizontalHeader().setFont(header_font)

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

        self.table_widget.setItemDelegate(StrongEditorDelegate(self.table_widget))

        c.addWidget(self.table_widget)
        v.addWidget(content)
        return right

    # ==================================================================
    # 分页辅助
    # ==================================================================
    def _update_pager_display(self):
        """更新分页显示：按钮启用状态 + 输入框里的 “当前页/总页” 文本"""
        self.total_pages = max(1, (self.total_rows + self.page_size - 1) // self.page_size)
        if self.current_page > self.total_pages:
            self.current_page = self.total_pages

        if self.page_input:
            self.page_input.setText(f"{self.current_page}/{self.total_pages}")

        if self.prev_btn:
            self.prev_btn.setEnabled(self.current_page > 1)
        if self.next_btn:
            self.next_btn.setEnabled(self.current_page < self.total_pages)

    def _refresh_current_page(self):
        """根据 base_sql + current_page + page_size 生成分页 SQL 并显示"""
        if not self.con:
            return
        offset = (self.current_page - 1) * self.page_size
        page_sql = f"SELECT * FROM ({self.base_sql}) sub LIMIT {self.page_size} OFFSET {offset}"
        self.current_sql = page_sql
        self.run_sql_to_table(page_sql)
        self._update_pager_display()
        self.status_label.setText(f"状态: 第 {self.current_page} 页查询成功")

    def _recount_total_rows(self):
        """根据 base_sql 重新统计总行数"""
        try:
            count_sql = f"SELECT COUNT(*) FROM ({self.base_sql}) sub"
            self.total_rows = self.con.execute(count_sql).fetchone()[0]
        except Exception:
            try:
                self.total_rows = self.con.execute(
                    f"SELECT COUNT(*) FROM ({self.base_sql})"
                ).fetchone()[0]
            except Exception:
                self.total_rows = self.table_widget.rowCount()

    def _prepare_base_sql_from_input(self):
        """
        从输入框中取出 SQL，去掉尾部 LIMIT / OFFSET，作为 base_sql。
        若用户写了 LIMIT N，则把 N 当作 page_size。
        """
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

    # ==================================================================
    # 排序 & CSV 导出
    # ==================================================================
    def on_header_clicked(self, logical_index):
        """点击列头进行排序，并同步更新 SQL 输入框"""
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
            limit_clause = "LIMIT 100"

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
            arrow = "↑" if self.sort_order == Qt.SortOrder.AscendingOrder else "↓"
            self.status_label.setText(
                f"状态: 按 {col_name} {arrow} 排序，当前页 {self.table_widget.rowCount()} 行"
            )
        except Exception as e:
            QMessageBox.warning(self, "排序错误", f"排序失败:\n{e}")

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
            cols = [
                self.table_widget.horizontalHeaderItem(i).text()
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

            self.con.execute(
                f"COPY __tmp_csv__ TO '{file_path.replace('\\', '/')}' "
                "(HEADER, DELIMITER ',');"
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
        """导出全部数据为 CSV（从 VIEW t）"""
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
            total_rows = self.con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
            self.con.execute(
                f"COPY t TO '{file_path.replace('\\', '/')}' (HEADER, DELIMITER ',');"
            )
            QMessageBox.information(
                self, "成功", f"全部数据已导出！\n共 {total_rows} 行"
            )
            self.status_label.setText(
                f"状态: 已导出全部数据到 {os.path.basename(file_path)}"
            )
        except Exception as e:
            QMessageBox.critical(self, "错误", f"导出失败:\n{e}")

    # ==================================================================
    # 数据读写 / 更新树
    # ==================================================================
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
            self.con.execute(
                f"CREATE VIEW t AS SELECT * FROM parquet_scan('{file_path.replace('\\', '/')}');"
            )

            meta = self.con.execute("SELECT * FROM t LIMIT 1")
            self.columns = [desc[0] for desc in meta.description] if meta.description else []

            file_name = os.path.basename(file_path)
            size_mb = os.path.getsize(file_path) / 1024 / 1024

            self.base_sql = "SELECT * FROM t"
            self.page_size = 100
            self.current_page = 1
            self.total_rows = self.con.execute("SELECT COUNT(*) FROM t").fetchone()[0]

            self.file_info_label.setText(
                f"文件: {file_name}\n大小: {size_mb:.2f} MB\n行数: {self.total_rows}\n列数: {len(self.columns)}"
            )

            self.update_tree()
            self._refresh_current_page()
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

        base_size = get_base_font_size()

        root = QTreeWidgetItem(self.tree_widget)
        root.setText(0, "数据表")
        root.setFont(0, QFont(get_base_font_family(), base_size + 1, QFont.Weight.Bold))

        columns_node = QTreeWidgetItem(root)
        columns_node.setText(0, "列 (Columns)")
        columns_node.setFont(0, QFont(get_base_font_family(), base_size + 1, QFont.Weight.Bold))

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
                        res = self.con.execute(
                            f"SELECT typeof({ident}) FROM t LIMIT 1"
                        ).fetchone()
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

    # ==================================================================
    # 显示数据
    # ==================================================================
    def run_sql_to_table(self, sql: str):
        res = self.con.execute(sql)
        self.columns = [desc[0] for desc in res.description] if res.description else []
        rows = [dict(zip(self.columns, row)) for row in res.fetchall()]
        self.table_cache = rows
        self.display_data(self.columns, rows)

    def display_data(self, columns, rows):
        """显示数据 + 自动分布列宽（自适应内容并占满表格宽度）"""
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

        # 自动列宽：先按内容自适应，再让最后一列 stretch 占满剩余空间
        header = self.table_widget.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        header.setStretchLastSection(True)

    # ==================================================================
    # 交互：执行 SQL、分页按钮
    # ==================================================================
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
        """从输入框跳转到指定页：支持 '5' 或 '5/32' 形式"""
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

    # ==================================================================
    # 表格编辑 / 保存
    # ==================================================================
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
        self.total_rows += 1
        self._update_pager_display()

    def delete_selected(self):
        rows = sorted({it.row() for it in self.table_widget.selectedItems()}, reverse=True)
        if not rows:
            QMessageBox.information(self, "提示", "请先选择要删除的行")
            return
        for r in rows:
            self.table_widget.removeRow(r)
        self.status_label.setText(f"状态: 已删除 {len(rows)} 行")
        self.total_rows = max(0, self.total_rows - len(rows))
        self._update_pager_display()

    def reset_view(self):
        if not self.con:
            return
        try:
            self.sort_column = None
            self.sort_order = Qt.SortOrder.AscendingOrder
            self.base_sql = "SELECT * FROM t"
            self.page_size = 100
            self.current_page = 1
            self._recount_total_rows()
            self.sql_input.setText("SELECT * FROM t LIMIT 100")
            self._refresh_current_page()
        except Exception as e:
            QMessageBox.warning(self, "错误", f"重置失败: {e}")

    def save_file(self):
        """
        保存为 Parquet：
        - 拉取 base_sql 对应的“全表”数据
        - 用当前页的编辑结果覆盖对应的行
        - 使用原表 t 的字段类型写出 Parquet
        - 默认文件名为当前打开的 parquet 文件（方便覆盖保存）
        """
        if self.table_widget.columnCount() == 0:
            QMessageBox.information(self, "提示", "没有数据可保存。")
            return

        if not self.con:
            QMessageBox.information(self, "提示", "尚未加载任何数据。")
            return

        normalized = " ".join(self.base_sql.split()).strip().upper()
        if normalized != "SELECT * FROM T":
            reply = QMessageBox.question(
                self,
                "保存提示",
                "当前 SQL 不是简单的 `SELECT * FROM t`。\n\n"
                "保存时会按照当前 SQL 的结果集构造“全表”，并用当前页的修改覆盖对应行，"
                "这可能会和原始 parquet 行顺序/行数不完全一致。\n\n"
                "建议：先点击“🔄 重置视图”再保存，以确保保存的是完整原始表。\n\n"
                "是否继续当前保存方式？",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply != QMessageBox.StandardButton.Yes:
                return

        # 默认保存路径 = 当前文件路径（包括文件名），方便直接覆盖原文件
        default_path = self.file_path or ""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "保存 Parquet 文件", default_path, "Parquet Files (*.parquet)"
        )
        if not file_path:
            return

        try:
            # 1. 拉取“全表”数据（基于 base_sql）
            rel = self.con.execute(self.base_sql)
            all_cols = [d[0] for d in rel.description] if rel.description else []
            all_rows = [list(row) for row in rel.fetchall()]
            total = len(all_rows)

            # 2. 用当前页的内容覆盖对应的行
            offset = (self.current_page - 1) * self.page_size
            page_rows = self.table_widget.rowCount()
            page_cols = self.table_widget.columnCount()

            for r in range(page_rows):
                global_idx = offset + r
                row_vals = []
                for c in range(page_cols):
                    it = self.table_widget.item(r, c)
                    text = it.text() if it else ""
                    row_vals.append(None if text == "" else text)
                if global_idx < total:
                    all_rows[global_idx] = row_vals
                else:
                    all_rows.append(row_vals)

            # 3. 在 DuckDB 中构造一个具有正确字段类型的临时表
            self._ensure_con()
            self.con.execute("DROP TABLE IF EXISTS __tmp_edit__;")
            self.con.execute("CREATE TABLE __tmp_edit__ AS SELECT * FROM t WHERE 1=0;")

            if len(all_cols) != len(self.con.execute("SELECT * FROM __tmp_edit__ LIMIT 0").description):
                raise RuntimeError("列数与原始表不一致，请先重置视图后再保存。")

            placeholders = ", ".join(["?"] * len(all_cols))
            self.con.executemany(
                f"INSERT INTO __tmp_edit__ VALUES ({placeholders})", all_rows
            )

            # 4. 写出 Parquet 文件
            self.con.execute(
                f"COPY __tmp_edit__ TO '{file_path.replace('\\', '/')}' (FORMAT PARQUET);"
            )

            QMessageBox.information(self, "成功", "文件已保存（包含全表数据）。")
            self.status_label.setText(f"状态: 已保存到 {os.path.basename(file_path)}")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"保存失败:\n{e}")


# ======================================================================
# 主窗口
# ======================================================================
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
        self.setWindowTitle("Parquet 文件查看器 (DuckDB) - 增强版")
        self.setGeometry(50, 50, 1300, 760)

        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        toolbar = QWidget()
        toolbar.setObjectName("mainToolbar")
        toolbar_layout = QHBoxLayout(toolbar)
        toolbar_layout.setContentsMargins(16, 8, 16, 8)
        toolbar_layout.setSpacing(10)

        open_btn = QPushButton("📂 打开文件")
        open_btn.setStyleSheet("padding: 6px 18px; font-size: 11pt; font-weight: 600;")
        open_btn.clicked.connect(self.open_file)
        toolbar_layout.addWidget(open_btn)

        new_tab_btn = QPushButton("➕ 新建标签")
        new_tab_btn.setStyleSheet("padding: 6px 18px; font-size: 11pt;")
        new_tab_btn.clicked.connect(self.new_tab)
        toolbar_layout.addWidget(new_tab_btn)

        toolbar_layout.addStretch()

        close_tab_btn = QPushButton("✖ 关闭当前标签")
        close_tab_btn.setStyleSheet("padding: 6px 18px; font-size: 11pt;")
        close_tab_btn.clicked.connect(self.close_current_tab)
        toolbar_layout.addWidget(close_tab_btn)

        main_layout.addWidget(toolbar)

        self.tab_widget = QTabWidget()
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.setMovable(True)
        self.tab_widget.tabCloseRequested.connect(self.close_tab)
        main_layout.addWidget(self.tab_widget)

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
            QPushButton:disabled {
                background-color: #9ca3af;
                color: #e5e7eb;
            }
            QLineEdit {
                background-color: #ffffff;
                border: 1px solid #d1d5db;
                border-radius: 6px;
                padding: 6px 10px;
                color: #111827;
                font-size: 11pt;
            }
            QLineEdit:focus {
                border: 2px solid #3b82f6;
                padding: 5px 9px;
            }
            QTableWidget {
                background-color: #ffffff;
                border: 1px solid #e5e7eb;
                border-radius: 8px;
                gridline-color: #f3f4f6;
            }
            QTableWidget::item {
                padding: 4px;
                border-bottom: 1px solid #f3f4f6;
            }
            QTableWidget::item:selected {
                background-color: #dbeafe;
                color: #1e40af;
            }
            QHeaderView::section {
                background-color: #f9fafb;
                color: #374151;
                padding: 6px;
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
                font-size: 11pt;
            }
            QTreeWidget::item {
                padding: 4px;
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
                padding: 8px 18px;
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
            if file_path.lower().endswith(".parquet"):
                self.open_file_in_new_tab(file_path)


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    base_font = QFont(get_base_font_family(), get_base_font_size())
    app.setFont(base_font)

    viewer = ParquetViewer()

    # 支持命令行双击 .parquet 直接打开
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg.lower().endswith(".parquet") and os.path.exists(arg):
            viewer.open_file_in_new_tab(arg)

    viewer.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
