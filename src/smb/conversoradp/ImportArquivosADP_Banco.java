package smb.conversoradp;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;

import smb.exception.MsgRetorno;
import smb.exception.PadraoException;
import smb.utils.ConexaoDiretaBanco;
import smb.utils.Utilitario;

public class ImportArquivosADP_Banco {

	public static void iterationFiles(ConexaoDiretaBanco con, String folder, Boolean createTable) throws PadraoException {

		// Reading the directory
		File fileFolder = new File(folder);

		// Read all files
		File[] listFiles = fileFolder.listFiles();

		// Creating tables based on 'estrutura' file
		if (createTable) {

			// Verifing if exists the 'estrutura' file
			Boolean havaStructureFile = false;
			for (int i = 0; i < listFiles.length; i++) {

				String fileName = listFiles[i].getName().replaceAll(".d", "").replaceAll(".txt", "");
				if (fileName.equalsIgnoreCase("estrutura")) {

					havaStructureFile = true;
				}
			}

			// Error return
			if (!havaStructureFile) {
				throw new PadraoException(new MsgRetorno("Não tem o arquivo de estrutura. Incluir o arquivo de estrutura e executar novamente\n"));
			}

			createTableScript(fileFolder, con);
		}

		// Iteration by file - Insert Files
		for (int i = 0; i < listFiles.length; i++) {

			String fileName = removeExtensionFromFileName(listFiles[i].getName());

			System.out.println("\tComeco: " + fileName);

			// Structure file, not insert
			if (fileName.equalsIgnoreCase("estrutura")) {

			} else if (fileName.equalsIgnoreCase("folha")) { // file need convert

				// Reading the file
				convertFolha(listFiles[i], con, fileName);

			} else if (fileName.equalsIgnoreCase("estab-empresa")) { // file need convert

				// Reading the file
				String fullFile;
				try {
					fullFile = new String(Files.readAllBytes(listFiles[i].toPath()), StandardCharsets.ISO_8859_1);
				} catch (IOException e) {
					throw new PadraoException(new MsgRetorno("Erro ao ler o arquivo: " + fileName));
				}

				// Split by rows
				String[] arrLinhas = Utilitario.textToLines(fullFile);
				
				int maxRowsInsert = 0;
				StringBuilder sb = new StringBuilder();
				// Iteration - INSERT
				for (int j = 1; j < arrLinhas.length; j++) {

					if ((j + 1) == arrLinhas.length || maxRowsInsert == 995) {

						String header = "INSERT INTO \n\t[" + fileName + "]\nVALUES\n";
						sb.append(insertQuery(arrLinhas[j].split(";")));

						con.executaSql(header + sb.toString().substring(0, sb.toString().length() - 1), true);

						maxRowsInsert = 0;
						sb = new StringBuilder();

					} else {

						sb.append(insertQuery(arrLinhas[j]));
						maxRowsInsert++;
					}
				}

			} else {

				// Reading the file
				String fullFile;
				try {
					fullFile = new String(Files.readAllBytes(listFiles[i].toPath()), StandardCharsets.ISO_8859_1);
				} catch (IOException e) {
					throw new PadraoException(new MsgRetorno("Erro ao ler o arquivo: " + fileName));
				}

				// Split by rows
				String[] arrLinhas = Utilitario.textToLines(fullFile);

				int maxRowsInsert = 0;
				StringBuilder sb = new StringBuilder();
				// Iteration - INSERT
				for (int j = 0; j < arrLinhas.length; j++) {

					if ((j + 1) == arrLinhas.length || maxRowsInsert == 995) {

						String header = "INSERT INTO \n\t[" + fileName + "]\nVALUES\n";
						sb.append(insertQuery(arrLinhas[j]));

						con.executaSql(header + sb.toString().substring(0, sb.toString().length() - 1), true);

						maxRowsInsert = 0;
						sb = new StringBuilder();

					} else {

						sb.append(insertQuery(arrLinhas[j]));
						maxRowsInsert++;
					}
				}
			}

			System.out.println("\tFim: " + fileName);
		}

	}

	private static void createTableScript(File fileFolder, ConexaoDiretaBanco con) throws PadraoException {

		HashMap<String, String> rowsFile = new HashMap<String, String>();

		File[] listFiles = fileFolder.listFiles();

		// iteration over files - get structure file
		for (int i = 0; i < listFiles.length; i++) {

			if (listFiles[i].getName().toLowerCase().contains("estrutura")) {

				// Reading the file
				String fullFile;
				try {
					fullFile = new String(Files.readAllBytes(listFiles[i].toPath()), StandardCharsets.ISO_8859_1);
				} catch (IOException e) {
					throw new PadraoException(new MsgRetorno("Erro ao ler o arquivo: " + listFiles[i].getName()));
				}

				String[] lines = fullFile.split("\r\n");
				StringBuilder sb = new StringBuilder();
				int separators = 0;
				String tableName = null;

				for (int j = 0; j < lines.length; j++) {

					if (j == 0) {

						tableName = lines[j];

					} else if (lines[j].toLowerCase().contains("---") && separators == 0) {

						separators++;

					} else if (lines[j].toLowerCase().contains("---") && separators == 1) {

						rowsFile.put(tableName, sb.toString().trim());

						sb = new StringBuilder();
						separators = 0;

					} else if ((j + 1) == lines.length) {

						sb.append(lines[j].substring(0, 33).trim().replace("[", "").replace("]", ""));
						rowsFile.put(tableName, sb.toString().trim());

						sb = new StringBuilder();
						separators = 0;

					} else if (separators == 1) {

						sb.append(lines[j].substring(0, 33).trim().replace("[", "").replace("]", ""));
						sb.append(" ");

					} else {

						tableName = lines[j];
					}
				}
			}
		}

		// iteration over files
		for (int i = 0; i < listFiles.length; i++) {

			String fileName = removeExtensionFromFileName(listFiles[i].getName());

			if (!fileName.equalsIgnoreCase("estrutura")) {

				String fullFile;
				try {
					fullFile = new String(Files.readAllBytes(listFiles[i].toPath()), StandardCharsets.ISO_8859_1);
				} catch (IOException e) {
					throw new PadraoException(new MsgRetorno("Erro ao ler o arquivo: " + listFiles[i].getName()));
				}

				if (fileName.equalsIgnoreCase("folha")) {

					String createTableQuery = createTableQuery(rowsFile.get(fileName), fileName, 150);
					con.executaSql(createTableQuery, true);

				} else if (fileName.equalsIgnoreCase("estab-empresa")) {

					String[] lines = Utilitario.textToLines(fullFile);

					Integer maxColumnLength = 0;
					// Iteration - COUNT MAX COLUMN LENGTH
					for (int j = 1; j < lines.length; j++) {

						String[] columns = lines[j].split(";");
						for (int k = 0; k < columns.length; k++) {

							if (columns[k].length() > maxColumnLength) {
								maxColumnLength = columns[k].length();
							}
						}
					}

					// creating query and insert into database
					String createTableQuery = createTableQuery(lines[0].split(";"), fileName, maxColumnLength);
					con.executaSql(createTableQuery, true);

				} else {

					// Split by rows
					String[] arrLinhas = Utilitario.textToLines(fullFile);

					Integer maxColumnLength = 0;
					// Iteration - COUNT MAX COLUMN LENGTH
					for (int j = 0; j < arrLinhas.length; j++) {

						String[] columns = Utilitario.textToColumns(arrLinhas[j]);
						for (int k = 0; k < columns.length; k++) {

							if (columns[k].length() > maxColumnLength) {
								maxColumnLength = columns[k].length();
							}
						}
					}

					System.out.println(fileName);

					// creating query and insert into database
					String createTableQuery = createTableQuery(rowsFile.get(fileName), fileName, maxColumnLength);
					con.executaSql(createTableQuery, true);
				}
			}
		}
	}

	private static String removeExtensionFromFileName(String name) {

		return name.toLowerCase().trim().replace(".d", "").replace(".txt", "");
	}

	private static String createTableQuery(String[] columns, String fileName, Integer maxColumnLength) {

		StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE [").append(fileName).append("] (\n\n");

		for (int i = 0; i < columns.length; i++) {

			if ((i + 1) == columns.length) {

				sb.append("\t[").append(columns[i]).append("] VARCHAR(").append(maxColumnLength).append(") NULL\n");

			} else {

				sb.append("\t[").append(columns[i]).append("] VARCHAR(").append(maxColumnLength).append(") NULL,\n");
			}
		}

		sb.append(");");

		return sb.toString();
	}

	private static String createTableQuery(String row, String fileName, Integer maxColumnLength) {

		String[] columns = Utilitario.textToColumns(row);

		StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE [").append(fileName).append("] (\n\n");

		for (int i = 0; i < columns.length; i++) {

			if ((i + 1) == columns.length) {

				sb.append("\t[").append(columns[i]).append("] VARCHAR(").append(maxColumnLength).append(") NULL\n");

			} else {

				sb.append("\t[").append(columns[i]).append("] VARCHAR(").append(maxColumnLength).append(") NULL,\n");
			}
		}

		sb.append(");");

		return sb.toString();
	}

	private static void convertFolha(File folha, ConexaoDiretaBanco con, String fileName) throws PadraoException {

		String fullStringFile;
		try {
			fullStringFile = new String(Files.readAllBytes(folha.toPath()), StandardCharsets.ISO_8859_1);
		} catch (IOException e) {
			throw new PadraoException(new MsgRetorno("Erro ao ler o arquivo: " + fileName));
		}

		// Variables
		StringBuilder sb = new StringBuilder();
		int maxRowsInsert = 0;

		// Split by rows
		String[] arrRows = Utilitario.textToLines(fullStringFile.replaceAll(" \\|", "\\|"));

		fullStringFile = null;

		// For by rows
		for (int i = 0; i < arrRows.length; i++) {

			// Split by columns
			String[] columns = Utilitario.textToColumns(arrRows[i]);

			// count how many rows will be
			int qtdItens = 0;
			for (int j = 0; j < columns.length; j++) {

				if (columns[j].contains("|")) {

					// Count |
					String[] itens = columns[j].split("\\|");

					// Getting the column with more pipes
					if (itens.length > qtdItens) {
						qtdItens = itens.length;
					}
				}
			}

			// If does not exists itens
			if (qtdItens == 0) {

				sb.append(insertQuery(columns));

				// Insert database
				maxRowsInsert++;
				if ((i + 1) == arrRows.length || maxRowsInsert == 990) {

					String header = "INSERT INTO \n\t[" + fileName + "]\nVALUES\n";
					con.executaSql(header + sb.toString().substring(0, sb.toString().length() - 1), true);

					maxRowsInsert = 0;
					sb = new StringBuilder();
				}

			} else {

				// Duplicando as linhas, na quantidade de vezes necessarias - de acordo com os
				// pipes
				String[] auxColumns = new String[columns.length];
				for (int j = 0; j < qtdItens; j++) {

					// Percorrendo as colunas
					for (int k = 0; k < columns.length; k++) {

						// Adicionando a coluna ou o item
						if (columns[k].contains("|")) {

							String[] itens = columns[k].split("\\|");

							if (itens.length > j) {
								auxColumns[k] = itens[j];
							}

						} else {

							auxColumns[k] = columns[k];
						}
					}

					sb.append(insertQuery(auxColumns));
					auxColumns = new String[columns.length];

					// Insert database
					maxRowsInsert++;
					if ((i + 1) == arrRows.length || maxRowsInsert == 990) {

						String header = "INSERT INTO \n\t[" + fileName + "]\nVALUES\n";
						con.executaSql(header + sb.toString().substring(0, sb.toString().length() - 1), true);

						maxRowsInsert = 0;
						sb = new StringBuilder();
					}
				}
			}
		}
	}

	private static String insertQuery(String row) {

		String[] columns = Utilitario.textToColumns(row);

		StringBuilder sb = new StringBuilder();
		sb.append("\t(");

		for (int i = 0; i < columns.length; i++) {

			String aux = columns[i].replaceAll("\"", "").replaceAll("'", "''");

			if ((i + 1) == columns.length) {

				sb.append("'").append(aux).append("'");

			} else {

				sb.append("'").append(aux).append("', ");
			}
		}
		sb.append("),");

		return sb.toString();
	}

	private static String insertQuery(String[] columns) {

		StringBuilder sb = new StringBuilder();
		sb.append("\t(");

		for (int i = 0; i < columns.length; i++) {

			String aux = columns[i].replaceAll("\"", "").replaceAll("'", "''");

			if ((i + 1) == columns.length) {

				sb.append("'").append(aux).append("'");

			} else {

				sb.append("'").append(aux).append("', ");
			}
		}
		sb.append("),");

		return sb.toString();
	}
}