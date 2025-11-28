import sys
from pathlib import Path
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QTableWidget, QTableWidgetItem,
                             QPushButton, QLineEdit, QLabel, QSplitter,
                             QTreeWidget, QTreeWidgetItem, QHeaderView,
                             QMessageBox, QFileDialog, QTabWidget)
from PyQt6.QtCore import Qt, QSettings, QUrl, QTimer
from PyQt6.QtGui import QColor, QFont, QDragEnterEvent, QDropEvent, QIcon, QGuiApplication, QCursor
import pandas as pd
import os


# ---------- 资源定位：兼容开发环境与 PyInstaller(onefile) ----------
def resource_path(relative: str) -> str:
    """
    获取资源文件路径：
    - 开发环境：当前工作目录
    - PyInstaller onefile：临时目录 sys._MEIPASS
    """
    if hasattr(sys, "_MEIPASS"):
        return str(Path(sys._MEIPASS) / relative)
    return str(Path.cwd() / relative)


class ParquetTab(QWidget):
    """单个 Parquet 文件的标签页"""

    def __init__(self, file_path=None):
        super().__init__()
        self.df = None
        self.original_df = None
        self.file_path = file_path
        self.init_ui()

        if file_path:
            self.load_file(file_path)

    def init_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # 创建分割器
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # 左侧面板
        left_panel = self.create_left_panel()
        splitter.addWidget(left_panel)

        # 右侧面板
        right_panel = self.create_right_panel()
        splitter.addWidget(right_panel)

        splitter.setSizes([280, 1120])
        layout.addWidget(splitter)

    def create_left_panel(self):
        """创建左侧文件结构面板"""
        left_widget = QWidget()
        left_widget.setMaximumWidth(280)
        left_widget.setObjectName("leftPanel")
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(0)

        # 标题
        title_widget = QWidget()
        title_widget.setObjectName("titleWidget")
        title_layout = QVBoxLayout(title_widget)
        title_layout.setContentsMargins(15, 15, 15, 15)

        title_label = QLabel("文件结构")
        title_label.setFont(QFont("Microsoft YaHei UI", 11, QFont.Weight.Bold))
        title_layout.addWidget(title_label)

        # 文件信息卡片
        info_card = QWidget()
        info_card.setObjectName("infoCard")
        info_card_layout = QVBoxLayout(info_card)
        info_card_layout.setContentsMargins(10, 10, 10, 10)

        self.file_info_label = QLabel("未加载文件")
        self.file_info_label.setFont(QFont("Microsoft YaHei UI", 8))
        self.file_info_label.setWordWrap(True)
        self.file_info_label.setStyleSheet("color: #6b7280;")
        info_card_layout.addWidget(self.file_info_label)

        title_layout.addWidget(info_card)
        left_layout.addWidget(title_widget)

        # 文件信息树
        self.tree_widget = QTreeWidget()
        self.tree_widget.setHeaderLabels(["名称 (Name)", "类型 (Type)"])
        self.tree_widget.setColumnWidth(0, 150)
        self.tree_widget.setIndentation(15)
        left_layout.addWidget(self.tree_widget)

        return left_widget

    def create_right_panel(self):
        """创建右侧数据视图面板"""
        right_widget = QWidget()
        right_widget.setObjectName("rightPanel")
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(0)

        # 顶部工具栏
        toolbar = QWidget()
        toolbar.setObjectName("toolbar")
        toolbar_layout = QHBoxLayout(toolbar)
        toolbar_layout.setContentsMargins(20, 15, 20, 15)

        # 文件名标签 - 左侧带图标
        file_info_layout = QHBoxLayout()
        file_icon = QLabel("📄")
        file_icon.setFont(QFont("Segoe UI Emoji", 12))
        file_info_layout.addWidget(file_icon)

        self.file_label = QLabel("未打开文件")
        self.file_label.setFont(QFont("Microsoft YaHei UI", 10))
        self.file_label.setStyleSheet("color: #374151;")
        file_info_layout.addWidget(self.file_label)
        file_info_layout.addStretch()

        toolbar_layout.addLayout(file_info_layout)
        toolbar_layout.addStretch()

        # 操作按钮
        btn_style = "padding: 7px 16px; font-size: 9pt;"

        add_btn = QPushButton("➕ 新增行")
        add_btn.setStyleSheet(btn_style)
        add_btn.clicked.connect(self.add_row)
        toolbar_layout.addWidget(add_btn)

        delete_btn = QPushButton("🗑️ 删除选中")
        delete_btn.setStyleSheet(btn_style)
        delete_btn.clicked.connect(self.delete_selected)
        toolbar_layout.addWidget(delete_btn)

        reset_btn = QPushButton("🔄 重置视图")
        reset_btn.setStyleSheet(btn_style)
        reset_btn.clicked.connect(self.reset_view)
        toolbar_layout.addWidget(reset_btn)

        save_btn = QPushButton("💾 保存")
        save_btn.setStyleSheet(btn_style + "background-color: #059669;")
        save_btn.clicked.connect(self.save_file)
        toolbar_layout.addWidget(save_btn)

        right_layout.addWidget(toolbar)

        # 内容区域
        content_widget = QWidget()
        content_widget.setObjectName("contentWidget")
        content_layout = QVBoxLayout(content_widget)
        content_layout.setContentsMargins(20, 15, 20, 20)
        content_layout.setSpacing(12)

        # SQL 查询区域
        sql_label = QLabel("SQL:")
        sql_label.setFont(QFont("Microsoft YaHei UI", 9, QFont.Weight.Bold))
        sql_label.setStyleSheet("color: #374151;")
        content_layout.addWidget(sql_label)

        sql_input_layout = QHBoxLayout()
        sql_input_layout.setSpacing(10)

        self.sql_input = QLineEdit()
        self.sql_input.setPlaceholderText("输入 SQL 查询... (例如: SELECT * FROM df WHERE indexcode = '000010')")
        self.sql_input.setText("SELECT * FROM df")
        self.sql_input.setMinimumHeight(38)
        self.sql_input.returnPressed.connect(self.run_query)
        sql_input_layout.addWidget(self.sql_input)

        run_btn = QPushButton("▶ 运行")
        run_btn.setMinimumWidth(90)
        run_btn.setMinimumHeight(38)
        run_btn.setStyleSheet("font-size: 9pt; padding: 0 24px; font-weight: 600;")
        run_btn.clicked.connect(self.run_query)
        sql_input_layout.addWidget(run_btn)

        content_layout.addLayout(sql_input_layout)

        # 状态标签
        self.status_label = QLabel("状态: 就绪")
        self.status_label.setFont(QFont("Microsoft YaHei UI", 8))
        self.status_label.setStyleSheet("color: #6b7280; padding: 5px 0;")
        content_layout.addWidget(self.status_label)

        # 数据表格
        self.table_widget = QTableWidget()
        self.table_widget.setAlternatingRowColors(True)
        self.table_widget.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table_widget.verticalHeader().setDefaultSectionSize(36)
        self.table_widget.verticalHeader().setMinimumSectionSize(36)  # 设置最小行高
        self.table_widget.setFont(QFont("Microsoft YaHei UI", 9))
        self.table_widget.setEditTriggers(QTableWidget.EditTrigger.DoubleClicked |
                                          QTableWidget.EditTrigger.EditKeyPressed |
                                          QTableWidget.EditTrigger.AnyKeyPressed)

        # 设置表格自适应列宽
        self.table_widget.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table_widget.horizontalHeader().setStretchLastSection(True)

        # 确保行号列也显示完整
        self.table_widget.verticalHeader().setVisible(True)
        self.table_widget.verticalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)

        content_layout.addWidget(self.table_widget)

        right_layout.addWidget(content_widget)

        return right_widget

    def load_file(self, file_path):
        """加载 Parquet 文件"""
        try:
            self.file_path = file_path
            self.df = pd.read_parquet(file_path)
            self.original_df = self.df.copy()

            file_name = os.path.basename(file_path)
            self.file_label.setText(file_name)

            # 更新文件信息
            file_size = os.path.getsize(file_path) / 1024 / 1024
            self.file_info_label.setText(
                f"文件: {file_name}\n"
                f"大小: {file_size:.2f} MB\n"
                f"行数: {len(self.df)}\n"
                f"列数: {len(self.df.columns)}"
            )

            self.update_tree()
            self.display_data(self.df)
            self.status_label.setText(f"状态: 成功加载 {len(self.df)} 行数据")

            return True
        except Exception as e:
            QMessageBox.critical(self, "错误", f"无法打开文件:\n{str(e)}")
            return False

    def update_tree(self):
        """更新文件结构树"""
        self.tree_widget.clear()

        if self.df is None:
            return

        root = QTreeWidgetItem(self.tree_widget)
        root.setText(0, f"数据表 ({len(self.df)} 行)")
        root.setFont(0, QFont("Microsoft YaHei UI", 9, QFont.Weight.Bold))

        columns_node = QTreeWidgetItem(root)
        columns_node.setText(0, "列 (Columns)")
        columns_node.setFont(0, QFont("Microsoft YaHei UI", 9, QFont.Weight.Bold))

        for col in self.df.columns:
            col_item = QTreeWidgetItem(columns_node)
            col_item.setText(0, col)
            col_item.setText(1, str(self.df[col].dtype))

        self.tree_widget.expandAll()

    def display_data(self, df):
        """显示数据并自适应列宽"""
        self.table_widget.clear()
        self.table_widget.setRowCount(len(df))
        self.table_widget.setColumnCount(len(df.columns))
        self.table_widget.setHorizontalHeaderLabels(df.columns.tolist())

        # 填充数据
        for i in range(len(df)):
            for j, col in enumerate(df.columns):
                value = df.iloc[i, j]

                # 格式化显示
                if pd.isna(value):
                    display_value = ""
                elif isinstance(value, float):
                    display_value = f"{value:.2f}"
                else:
                    display_value = str(value)

                item = QTableWidgetItem(display_value)

                # 数值着色
                if col in ['change', 'hang']:
                    try:
                        val = float(value)
                        if val > 0:
                            item.setForeground(QColor(220, 38, 38))
                        elif val < 0:
                            item.setForeground(QColor(22, 163, 74))
                    except Exception:
                        pass
                elif col in ['change_rate', 'hange_rat']:
                    try:
                        val = float(value)
                        if val > 0:
                            item.setForeground(QColor(220, 38, 38))
                        elif val < 0:
                            item.setForeground(QColor(22, 163, 74))
                    except Exception:
                        pass

                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table_widget.setItem(i, j, item)

        # 自适应列宽
        self.table_widget.resizeColumnsToContents()

        # 设置最小列宽，避免过窄
        for col in range(self.table_widget.columnCount()):
            current_width = self.table_widget.columnWidth(col)
            if current_width < 80:
                self.table_widget.setColumnWidth(col, 80)
            elif current_width > 200:
                self.table_widget.setColumnWidth(col, 200)

    def run_query(self):
        """执行 SQL 查询"""
        if self.df is None:
            QMessageBox.warning(self, "警告", "没有数据可查询")
            return

        query = self.sql_input.text().strip()
        if not query:
            return

        try:
            import sqlite3
            conn = sqlite3.connect(':memory:')
            self.original_df.to_sql('df', conn, index=False, if_exists='replace')
            result_df = pd.read_sql_query(query, conn)
            conn.close()

            self.display_data(result_df)
            self.status_label.setText(f"状态: 成功加载 {len(result_df)} 行数据")
        except Exception as e:
            QMessageBox.warning(self, "查询错误", f"SQL 查询失败:\n{str(e)}")

    def add_row(self):
        """添加新行"""
        if self.df is None:
            QMessageBox.warning(self, "警告", "没有数据")
            return

        # 在表格末尾插入新行
        row_count = self.table_widget.rowCount()
        self.table_widget.insertRow(row_count)

        # 为新行的每一列创建空白单元格（确保可见）
        for col in range(self.table_widget.columnCount()):
            item = QTableWidgetItem("")
            item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            item.setBackground(QColor(255, 251, 235))  # 淡黄色背景，表示新添加
            self.table_widget.setItem(row_count, col, item)

        # 滚动到新添加的行
        self.table_widget.scrollToItem(
            self.table_widget.item(row_count, 0),
            QTableWidget.ScrollHint.PositionAtBottom
        )

        # 选中新行
        self.table_widget.selectRow(row_count)

        # 设置焦点到第一列，方便用户直接编辑
        self.table_widget.setCurrentCell(row_count, 0)

        # 确保新行完全可见 - 调整行高
        self.table_widget.resizeRowToContents(row_count)

        self.status_label.setText(f"状态: 已添加新行 (第 {row_count + 1} 行)，可直接编辑")

    def delete_selected(self):
        """删除选中行"""
        selected_rows = set(item.row() for item in self.table_widget.selectedItems())

        if not selected_rows:
            QMessageBox.information(self, "提示", "请先选择要删除的行")
            return

        for row in sorted(selected_rows, reverse=True):
            self.table_widget.removeRow(row)

        self.status_label.setText(f"状态: 已删除 {len(selected_rows)} 行")

    def reset_view(self):
        """重置视图"""
        if self.original_df is not None:
            self.display_data(self.original_df)
            # 滚动到表格顶部
            self.table_widget.scrollToTop()
            self.status_label.setText(f"状态: 成功加载 {len(self.original_df)} 行数据")

    def save_file(self):
        """保存文件"""
        if self.df is None:
            QMessageBox.warning(self, "警告", "没有数据可保存")
            return

        default_path = self.file_path if self.file_path else ""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "保存 Parquet 文件", default_path, "Parquet Files (*.parquet)"
        )

        if file_path:
            try:
                data = {}
                for col_idx in range(self.table_widget.columnCount()):
                    col_name = self.table_widget.horizontalHeaderItem(col_idx).text()
                    col_data = []
                    for row_idx in range(self.table_widget.rowCount()):
                        item = self.table_widget.item(row_idx, col_idx)
                        col_data.append(item.text() if item else '')
                    data[col_name] = col_data

                df_to_save = pd.DataFrame(data)
                df_to_save.to_parquet(file_path)
                QMessageBox.information(self, "成功", "文件保存成功！")
                self.status_label.setText(f"状态: 已保存到 {os.path.basename(file_path)}")
            except Exception as e:
                QMessageBox.critical(self, "错误", f"保存失败:\n{str(e)}")


class ParquetViewer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.settings = QSettings("ParquetViewer", "Settings")
        self.recent_files = []
        self.load_settings()
        self.init_ui()

        # 应用窗口图标（保证标题栏/任务栏显示）
        icon_path = resource_path("ParquetViewer.ico")
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))

        # 启用拖放功能
        self.setAcceptDrops(True)

    def center_on_active_screen(self):
        """将窗口居中到当前活动屏幕（鼠标所在屏幕），退化到窗口当前屏或主屏。"""
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
        self.setWindowTitle('Parquet 文件查看器')
        self.setGeometry(100, 100, 1500, 850)

        # 主窗口部件
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # 顶部工具栏
        toolbar = QWidget()
        toolbar.setObjectName("mainToolbar")
        toolbar_layout = QHBoxLayout(toolbar)
        toolbar_layout.setContentsMargins(20, 12, 20, 12)

        # 左侧标题
        title_layout = QHBoxLayout()
        icon_label = QLabel("📊")
        icon_label.setFont(QFont("Segoe UI Emoji", 14))
        title_layout.addWidget(icon_label)

        title = QLabel("Parquet 文件查看器")
        title.setFont(QFont("Microsoft YaHei UI", 13, QFont.Weight.Bold))
        title_layout.addWidget(title)
        title_layout.addStretch()

        toolbar_layout.addLayout(title_layout)
        toolbar_layout.addStretch()

        # 右侧按钮
        open_btn = QPushButton("📁 打开文件")
        open_btn.clicked.connect(self.open_file)
        open_btn.setMinimumHeight(36)
        open_btn.setStyleSheet("font-size: 9pt; padding: 0 20px; font-weight: 600;")
        toolbar_layout.addWidget(open_btn)

        main_layout.addWidget(toolbar)

        # 标签页控件
        self.tab_widget = QTabWidget()
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.tabCloseRequested.connect(self.close_tab)
        self.tab_widget.setMovable(True)
        self.tab_widget.setDocumentMode(True)

        # 启用标签栏的双击新建功能
        self.tab_widget.tabBar().setTabsClosable(True)
        self.tab_widget.tabBar().tabBarDoubleClicked.connect(self.on_tab_bar_double_clicked)

        main_layout.addWidget(self.tab_widget)

        # 创建初始标签页
        self.new_tab()

        # 应用样式
        self.apply_styles()

        # 启动后异步居中到当前活动屏幕（确保几何就绪）
        QTimer.singleShot(0, self.center_on_active_screen)

    def new_tab(self):
        """创建新标签页"""
        tab = ParquetTab()
        index = self.tab_widget.addTab(tab, "未命名")
        self.tab_widget.setCurrentIndex(index)

    def on_tab_bar_double_clicked(self, index):
        """标签栏双击事件处理"""
        # 如果双击的是空白区域（index为-1），创建新标签
        if index == -1:
            self.new_tab()

    def open_file(self):
        """打开文件对话框"""
        last_dir = self.settings.value("last_directory", "")

        file_path, _ = QFileDialog.getOpenFileName(
            self, "选择 Parquet 文件", last_dir, "Parquet Files (*.parquet)"
        )

        if file_path:
            self.settings.setValue("last_directory", os.path.dirname(file_path))
            self.add_recent_file(file_path)
            self.open_file_in_tab(file_path)

    def open_file_in_tab(self, file_path):
        """在新标签页中打开文件"""
        # 检查是否已打开
        for i in range(self.tab_widget.count()):
            tab = self.tab_widget.widget(i)
            if isinstance(tab, ParquetTab) and tab.file_path == file_path:
                self.tab_widget.setCurrentIndex(i)
                return

        # 创建新标签页
        tab = ParquetTab()
        if tab.load_file(file_path):
            file_name = os.path.basename(file_path)

            # 替换空标签
            current_tab = self.tab_widget.currentWidget()
            if isinstance(current_tab, ParquetTab) and current_tab.df is None:
                index = self.tab_widget.currentIndex()
                self.tab_widget.removeTab(index)

            index = self.tab_widget.addTab(tab, file_name)
            self.tab_widget.setCurrentIndex(index)

    def close_tab(self, index):
        """关闭标签页"""
        if self.tab_widget.count() > 1:
            self.tab_widget.removeTab(index)
        else:
            self.tab_widget.removeTab(index)
            self.new_tab()

    def add_recent_file(self, file_path):
        """添加到最近文件"""
        if file_path in self.recent_files:
            self.recent_files.remove(file_path)

        self.recent_files.insert(0, file_path)
        self.recent_files = self.recent_files[:10]
        self.save_settings()

    def load_settings(self):
        """加载设置"""
        self.recent_files = self.settings.value("recent_files", [])
        if not isinstance(self.recent_files, list):
            self.recent_files = []

    def save_settings(self):
        """保存设置"""
        self.settings.setValue("recent_files", self.recent_files)

    def closeEvent(self, event):
        """关闭窗口时保存设置"""
        self.save_settings()
        event.accept()

    def dragEnterEvent(self, event: QDragEnterEvent):
        """拖拽进入事件"""
        if event.mimeData().hasUrls():
            # 检查是否有 .parquet 文件
            urls = event.mimeData().urls()
            for url in urls:
                file_path = url.toLocalFile()
                if file_path.lower().endswith('.parquet'):
                    event.acceptProposedAction()
                    return
        event.ignore()

    def dragMoveEvent(self, event):
        """拖拽移动事件"""
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(self, event: QDropEvent):
        """拖拽放下事件"""
        if event.mimeData().hasUrls():
            urls = event.mimeData().urls()
            parquet_files = []

            # 收集所有 .parquet 文件
            for url in urls:
                file_path = url.toLocalFile()
                if file_path.lower().endswith('.parquet') and os.path.exists(file_path):
                    parquet_files.append(file_path)

            # 打开所有文件
            if parquet_files:
                for file_path in parquet_files:
                    self.settings.setValue("last_directory", os.path.dirname(file_path))
                    self.add_recent_file(file_path)
                    self.open_file_in_tab(file_path)
                event.acceptProposedAction()
            else:
                QMessageBox.warning(self, "警告", "请拖入 .parquet 文件")
                event.ignore()

    def apply_styles(self):
        """应用样式表"""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f5f5;
            }
            QWidget#mainToolbar {
                background-color: white;
                border-bottom: 1px solid #e0e0e0;
            }
            QWidget#leftPanel {
                background-color: #fafafa;
                border-right: 1px solid #e0e0e0;
            }
            QWidget#rightPanel {
                background-color: white;
            }
            QWidget#titleWidget {
                background-color: #fafafa;
                border-bottom: 1px solid #e5e7eb;
            }
            QWidget#infoCard {
                background-color: white;
                border: 1px solid #e5e7eb;
                border-radius: 6px;
                margin-top: 8px;
            }
            QWidget#toolbar {
                background-color: white;
                border-bottom: 1px solid #e5e7eb;
            }
            QWidget#contentWidget {
                background-color: white;
            }
            QPushButton {
                background-color: #3b82f6;
                color: white;
                border: none;
                border-radius: 6px;
                font-family: "Microsoft YaHei UI";
            }
            QPushButton:hover {
                background-color: #2563eb;
            }
            QPushButton:pressed {
                background-color: #1d4ed8;
            }
            QLineEdit {
                padding: 10px 14px;
                border: 1px solid #d1d5db;
                border-radius: 6px;
                background-color: white;
                font-family: "Microsoft YaHei UI";
                font-size: 9pt;
            }
            QLineEdit:focus {
                border: 1.5px solid #3b82f6;
            }
            QTableWidget {
                background-color: white;
                border: 1px solid #e5e7eb;
                border-radius: 8px;
                gridline-color: #f0f0f0;
            }
            QTableWidget::item {
                padding: 6px;
                border: none;
            }
            QTableWidget::item:selected {
                background-color: #e0f2fe;
                color: #0c4a6e;
            }
            QTableWidget::item:alternate {
                background-color: #fafafa;
            }
            QTableWidget::item:focus {
                background-color: #fff7ed;
                border: 2px solid #3b82f6;
            }
            QLineEdit[readOnly="false"] {
                background-color: white;
                color: #1f2937;
                selection-background-color: #3b82f6;
                selection-color: white;
            }
            QTableWidget QLineEdit {
                background-color: white;
                color: #1f2937;
                border: 2px solid #3b82f6;
                padding: 2px 4px;
            }
            QTableWidget QTableCornerButton::section {
                background-color: #f9fafb;
                border: none;
                border-bottom: 1px solid #e5e7eb;
                border-right: 1px solid #e5e7eb;
            }
            QTableWidget::verticalHeader {
                background-color: #f9fafb;
            }
            QHeaderView::section:vertical {
                background-color: #f9fafb;
                padding: 4px;
                border: none;
                border-bottom: 1px solid #f0f0f0;
                border-right: 1px solid #e5e7eb;
                font-size: 8pt;
                color: #6b7280;
            }
            QHeaderView::section {
                background-color: #f9fafb;
                padding: 10px 12px;
                border: none;
                border-bottom: 1px solid #e5e7eb;
                border-right: 1px solid #f0f0f0;
                font-weight: 600;
                font-size: 9pt;
                color: #374151;
                font-family: "Microsoft YaHei UI";
            }
            QTreeWidget {
                background-color: white;
                border: none;
                border-top: 1px solid #e5e7eb;
                outline: none;
                font-size: 9pt;
            }
            QTreeWidget::item {
                padding: 6px 4px;
                border: none;
            }
            QTreeWidget::item:hover {
                background-color: #f3f4f6;
            }
            QTreeWidget::item:selected {
                background-color: #e0f2fe;
                color: #0c4a6e;
            }
            QTabWidget::pane {
                border: none;
                background-color: white;
                top: -1px;
            }
            QTabBar {
                background-color: #f5f5f5;
            }
            QTabBar::tab {
                background-color: #f5f5f5;
                color: #6b7280;
                padding: 10px 20px;
                margin-right: 2px;
                border-top-left-radius: 6px;
                border-top-right-radius: 6px;
                font-family: "Microsoft YaHei UI";
                font-size: 9pt;
            }
            QTabBar::tab:selected {
                background-color: white;
                color: #1f2937;
                font-weight: 500;
            }
            QTabBar::tab:hover:!selected {
                background-color: #e5e7eb;
            }
            QLabel {
                color: #374151;
            }
        """)


def main():
    app = QApplication(sys.argv)
    app.setApplicationName("Parquet Viewer")
    app.setOrganizationName("ParquetViewer")

    # 应用级图标（任务栏/切换器）
    ico_path = resource_path("app.ico")
    if os.path.exists(ico_path):
        app.setWindowIcon(QIcon(ico_path))

    viewer = ParquetViewer()
    viewer.show()

    # 处理命令行参数 - 支持双击 .parquet 打开
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        if os.path.exists(file_path) and file_path.lower().endswith('.parquet'):
            viewer.open_file_in_tab(file_path)

    sys.exit(app.exec())


if __name__ == '__main__':
    main()
