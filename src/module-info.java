module SMB_ConversorADP {
	requires javafx.controls;
	requires javafx.fxml;
	requires java.sql;

	opens smb to javafx.graphics, javafx.fxml;
}
