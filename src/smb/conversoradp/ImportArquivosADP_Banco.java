package smb.conversoradp;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import smb.exception.MsgRetorno;
import smb.exception.PadraoException;
import smb.utils.ConexaoDiretaBanco;
import smb.utils.Utilitario;

public class ImportArquivosADP_Banco {

	public static void iterationFiles(ConexaoDiretaBanco con, String folder, Boolean createTable)
			throws PadraoException {

		// Reading the directory
		File file = new File(folder);

		// Read all files
		File[] listFiles = file.listFiles();

		// Creating table based on 'estrutura' file
		if (createTable) {

			// Verifing if exists the 'estrutura' file
			Boolean havaStructureFile = false;
			int filePosition = 0;
			for (int i = 0; i < listFiles.length; i++) {

				String fileName = listFiles[i].getName().replaceAll(".d", "").replaceAll(".txt", "");
				if (fileName.equalsIgnoreCase("estrutura")) {

					havaStructureFile = true;
					filePosition = i;
				}
			}

			// Error return
			if (!havaStructureFile) {
				throw new PadraoException(new MsgRetorno("Não tem o arquivo de estrutura."));
			}

			createTableScript(listFiles[filePosition]);
		}

		// Iteration by file
		for (int i = 0; i < listFiles.length; i++) {

			String fileName = listFiles[i].getName().replaceAll(".d", "").replaceAll(".txt", "");

			System.out.println("\tComeço: " + fileName);

			// Alguns precisam de tratamento especial
			if (fileName.equalsIgnoreCase("folha")) {

				// Reading the file
				convertFolha(listFiles[i], con, fileName);

			} else if (fileName.equalsIgnoreCase("estab-empresa")) {

			} else {

				// Reading the file
				String fullFile;
				try {
					fullFile = new String(Files.readAllBytes(listFiles[i].toPath()), StandardCharsets.ISO_8859_1);
				} catch (IOException e) {
					throw new PadraoException(new MsgRetorno("Erro ao ler o arquivo: " + fileName));
				}

				// Split by rows
				String[] arrLinhas = fullFile.split("\r\n");

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

				int maxRowsInsert = 0;
				StringBuilder sb = new StringBuilder();
				// Iteration - CREATE AND INSERT
				for (int j = 0; j < arrLinhas.length; j++) {

					// First row
					if (j == 0) {

						String createTableQuery = createTableQuery(arrLinhas[j], fileName, maxColumnLength);
						con.executaSql(createTableQuery, true);

					} else { // Other rows

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
			}

			System.out.println("\tFim: " + fileName);
		}
	}

	private static void createTableScript(File file) {
		// TODO Auto-generated method stub
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
		String[] arrRows = fullStringFile.replaceAll(" \\|", "\\|").split("\r\n");

		fullStringFile = null;

		// For by rows
		for (int i = 0; i < arrRows.length; i++) {

			// Header
			if (i == 0) {

				// TODO

				String createTableQuery = createTableQuery(arrRows[i], fileName, 150);
				con.executaSql(createTableQuery, true);

			} else { // Rows

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