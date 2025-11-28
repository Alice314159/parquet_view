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
    QStyledItemDelegate
)
from PyQt6.QtCore import Qt, QSettings, QTimer
from PyQt6.QtGui import QColor, QFont, QDragEnterEvent, QDropEvent, QIcon, QGuiApplication, QCursor


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
                color: #111827;                /* 深灰文字 */
                border: 2px solid #2563eb;     /* 明显的蓝色边框 */
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
        editor.selectAll()  # 进入编辑自动全选，便于直接覆盖输入
# =======================================================================


class ParquetTab(QWidget):
    """单个 Parquet 文件标签页（DuckDB 版本，不依赖 pandas/pyarrow）"""
    def __init__(self, file_path=None):
        super().__init__()
        self.file_path = None
        self.con: duckdb.DuckDBPyConnection | None = None
        self.table_cache = None
        self.columns = []
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
        save_btn = QPushButton("💾 保存为 Parquet")
        save_btn.setStyleSheet(btn_style + "background-color: #059669;")
        save_btn.clicked.connect(self.save_file)

        for b in (add_btn, del_btn, reset_btn, save_btn):
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
        self.sql_input.setPlaceholderText("输入 SQL 查询... (例如: SELECT * FROM t LIMIT 100)")
        self.sql_input.setText("SELECT * FROM t LIMIT 100")
        self.sql_input.setMinimumHeight(38)
        self.sql_input.returnPressed.connect(self.run_query)   # Enter 直接执行
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

        # 安装“编辑态强化”委托
        self.table_widget.setItemDelegate(StrongEditorDelegate(self.table_widget))

        c.addWidget(self.table_widget)
        v.addWidget(content)
        return right

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
        """使用 DuckDB 直接对当前 parquet 做 DESCRIBE，更新文件结构树"""
        self.tree_widget.clear()

        # 没有文件就不显示
        if not getattr(self, "file_path", None):
            return

        # 确保有 duckdb 连接
        try:
            import duckdb
        except ImportError:
            # 真的没有 duckdb，就简单提示一下
            root = QTreeWidgetItem(self.tree_widget)
            root.setText(0, "数据表")
            columns_node = QTreeWidgetItem(root)
            columns_node.setText(0, "列 (Columns)")
            item = QTreeWidgetItem(columns_node)
            item.setText(0, "无法获取列信息")
            item.setText(1, "duckdb 未安装")
            self.tree_widget.expandAll()
            return

        if not getattr(self, "con", None):
            # 你如果在别处已经创建 self.con，这里就会跳过
            self.con = duckdb.connect()

        root = QTreeWidgetItem(self.tree_widget)
        root.setText(0, "数据表")
        root.setFont(0, QFont("Microsoft YaHei UI", 9, QFont.Weight.Bold))

        columns_node = QTreeWidgetItem(root)
        columns_node.setText(0, "列 (Columns)")
        columns_node.setFont(0, QFont("Microsoft YaHei UI", 9, QFont.Weight.Bold))

        try:
            # ⭐ 关键：直接针对当前 parquet 文件做 DESCRIBE，不再依赖 self.df / self.table_name
            rows = self.con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)",
                [self.file_path]
            ).fetchall()  # 每行: (column_name, column_type, null, key, default, extra)

            for name, col_type, *_ in rows:
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
        self.display_data(self.columns, rows)

    def display_data(self, columns, rows):
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

        self.table_widget.resizeColumnsToContents()
        for c in range(self.table_widget.columnCount()):
            w = self.table_widget.columnWidth(c)
            if w < 80:
                self.table_widget.setColumnWidth(c, 80)
            elif w > 220:
                self.table_widget.setColumnWidth(c, 220)

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
            it.setBackground(QColor(255, 255, 255))  # 新行底色改为白色，避免视觉冲突
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

        ico = resource_path("app.ico")
        if os.path.exists(ico):
            self.setWindowIcon(QIcon(ico))

        self.setAcceptDrops(True)

    # 多屏：始终居中到当前活动屏幕
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
        self.setWindowTitle('Parquet 文件查看器 (DuckDB)')
        self.setGeometry(100, 100, 1400, 820)

        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        toolbar = QWidget()
        toolbar.setObjectName("mainToolbar")
        tl = QHBoxLayout(toolbar)
        tl.setContentsMargins(20, 12, 20, 12)

        title_layout = QHBoxLayout()
        icon_label = QLabel("📊")
        icon_label.setFont(QFont("Segoe UI Emoji", 14))
        title_layout.addWidget(icon_label)

        title = QLabel("Parquet 文件查看器 (DuckDB)")
        title.setFont(QFont("Microsoft YaHei UI", 13, QFont.Weight.Bold))
        title_layout.addWidget(title)
        title_layout.addStretch()

        tl.addLayout(title_layout)
        tl.addStretch()

        open_btn = QPushButton("📁 打开文件")
        open_btn.clicked.connect(self.open_file)
        open_btn.setMinimumHeight(36)
        open_btn.setStyleSheet("font-size: 9pt; padding: 0 20px; font-weight: 600;")
        tl.addWidget(open_btn)

        main_layout.addWidget(toolbar)

        self.tab_widget = QTabWidget()
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.tabCloseRequested.connect(self.close_tab)
        self.tab_widget.setMovable(True)
        self.tab_widget.setDocumentMode(True)
        self.tab_widget.tabBar().setTabsClosable(True)
        self.tab_widget.tabBar().tabBarDoubleClicked.connect(self.on_tab_bar_double_clicked)
        main_layout.addWidget(self.tab_widget)

        self.new_tab()
        self.apply_styles()

        QTimer.singleShot(0, self.center_on_active_screen)

    def new_tab(self):
        tab = ParquetTab()
        idx = self.tab_widget.addTab(tab, "未命名")
        self.tab_widget.setCurrentIndex(idx)

    def on_tab_bar_double_clicked(self, index):
        if index == -1:
            self.new_tab()

    def open_file(self):
        last_dir = self.settings.value("last_directory", "")
        file_path, _ = QFileDialog.getOpenFileName(self, "选择 Parquet 文件", last_dir, "Parquet Files (*.parquet)")
        if file_path:
            self.settings.setValue("last_directory", os.path.dirname(file_path))
            self.add_recent_file(file_path)
            self.open_file_in_tab(file_path)

    def open_file_in_tab(self, file_path):
        for i in range(self.tab_widget.count()):
            tab = self.tab_widget.widget(i)
            if isinstance(tab, ParquetTab) and tab.file_path == file_path:
                self.tab_widget.setCurrentIndex(i)
                return
        tab = ParquetTab()
        if tab.load_file(file_path):
            file_name = os.path.basename(file_path)
            current_tab = self.tab_widget.currentWidget()
            if isinstance(current_tab, ParquetTab) and current_tab.file_path is None:
                self.tab_widget.removeTab(self.tab_widget.currentIndex())
            idx = self.tab_widget.addTab(tab, file_name)
            self.tab_widget.setCurrentIndex(idx)

    def close_tab(self, index):
        if self.tab_widget.count() > 1:
            self.tab_widget.removeTab(index)
        else:
            self.tab_widget.removeTab(index)
            self.new_tab()

    def add_recent_file(self, file_path):
        if file_path in self.recent_files:
            self.recent_files.remove(file_path)
        self.recent_files.insert(0, file_path)
        self.recent_files = self.recent_files[:10]
        self.save_settings()

    def load_settings(self):
        self.recent_files = self.settings.value("recent_files", [])
        if not isinstance(self.recent_files, list):
            self.recent_files = []

    def save_settings(self):
        self.settings.setValue("recent_files", self.recent_files)

    def closeEvent(self, event):
        self.save_settings()
        event.accept()

    def dragEnterEvent(self, event: QDragEnterEvent):
        if event.mimeData().hasUrls():
            for url in event.mimeData().urls():
                fp = url.toLocalFile()
                if fp.lower().endswith('.parquet'):
                    event.acceptProposedAction()
                    return
        event.ignore()

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(self, event: QDropEvent):
        if not event.mimeData().hasUrls():
            event.ignore()
            return
        files = []
        for url in event.mimeData().urls():
            fp = url.toLocalFile()
            if fp.lower().endswith('.parquet') and os.path.exists(fp):
                files.append(fp)
        if not files:
            QMessageBox.warning(self, "警告", "请拖入 .parquet 文件")
            event.ignore()
            return
        for fp in files:
            self.settings.setValue("last_directory", os.path.dirname(fp))
            self.add_recent_file(fp)
            self.open_file_in_tab(fp)
        event.acceptProposedAction()

    def apply_styles(self):
        self.setStyleSheet("""
            QMainWindow { background-color: #f5f5f5; }
            QWidget#mainToolbar { background-color: white; border-bottom: 1px solid #e0e0e0; }
            QWidget#leftPanel { background-color: #fafafa; border-right: 1px solid #e0e0e0; }
            QWidget#rightPanel { background-color: white; }
            QWidget#titleWidget { background-color: #fafafa; border-bottom: 1px solid #e5e7eb; }
            QWidget#infoCard { background-color: white; border: 1px solid #e5e7eb; border-radius: 6px; margin-top: 8px; }
            QWidget#toolbar { background-color: white; border-bottom: 1px solid #e5e7eb; }
            QWidget#contentWidget { background-color: white; }
            QPushButton { background-color: #3b82f6; color: white; border: none; border-radius: 6px; font-family: "Microsoft YaHei UI"; }
            QPushButton:hover { background-color: #2563eb; }
            QPushButton:pressed { background-color: #1d4ed8; }
            QLineEdit { padding: 10px 14px; border: 1px solid #d1d5db; border-radius: 6px; background-color: white; font-family: "Microsoft YaHei UI"; font-size: 9pt; }
            QLineEdit:focus { border: 1.5px solid #3b82f6; }
            QTableWidget { background-color: white; border: 1px solid #e5e7eb; border-radius: 8px; gridline-color: #f0f0f0; }
            QTableWidget::item { padding: 6px; border: none; }
            QTableWidget::item:selected { background-color: #eef6ff; color: #0c4a6e; }   /* 选中底色更浅 */
            QTableWidget::item:alternate { background-color: #fafafa; }
            QTableWidget::item:focus { background-color: #ffffff; border: none; }      /* 编辑态单元格保持白底 */
            QHeaderView::section { background-color: #f9fafb; padding: 10px 12px; border: none; border-bottom: 1px solid #e5e7eb; border-right: 1px solid #f0f0f0; font-weight: 600; font-size: 9pt; color: #374151; font-family: "Microsoft YaHei UI"; }
            QTreeWidget { background-color: white; border: none; border-top: 1px solid #e5e7eb; outline: none; font-size: 9pt; }
            QTreeWidget::item:hover { background-color: #f3f4f6; }
            QTreeWidget::item:selected { background-color: #e0f2fe; color: #0c4a6e; }
            QTabBar::tab { background-color: #f5f5f5; color: #6b7280; padding: 10px 20px; margin-right: 2px; border-top-left-radius: 6px; border-top-right-radius: 6px; font-family: "Microsoft YaHei UI"; font-size: 9pt; }
            QTabBar::tab:selected { background-color: white; color: #1f2937; font-weight: 500; }
            QTabBar::tab:hover:!selected { background-color: #e5e7eb; }
            QLabel { color: #374151; }
        """)


def main():
    # 高 DPI 适配
    os.environ.setdefault("QT_ENABLE_HIGHDPI_SCALING", "1")
    os.environ.setdefault("QT_AUTO_SCREEN_SCALE_FACTOR", "1")

    app = QApplication(sys.argv)
    app.setApplicationName("Parquet Viewer (DuckDB)")
    app.setOrganizationName("ParquetViewer")

    ico = resource_path("app.ico")
    if os.path.exists(ico):
        app.setWindowIcon(QIcon(ico))

    win = ParquetViewer()
    win.show()

    # 双击文件打开
    if len(sys.argv) > 1:
        fp = sys.argv[1]
        if os.path.exists(fp) and fp.lower().endswith(".parquet"):
            win.open_file_in_tab(fp)

    sys.exit(app.exec())


if __name__ == "__main__":
    main()

