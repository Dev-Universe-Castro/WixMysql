const { getConnection } = require('./mysqlConnector'); // Importe o novo conector MySQL

async function executeQuery(query, binds = []) {
    let connection;
    try {
        connection = await getConnection();
        console.log('Conexão estabelecida com o banco de dados.');

        // Logue a consulta SQL e os parâmetros
        console.log('Executando consulta:', query);
        console.log('Parâmetros:', binds);

        const [rows] = await connection.execute(query, binds);
        console.log('Resultados brutos:', rows);

        return rows || [];
    } catch (err) {
        console.error('Erro ao executar consulta:', err);
        throw err;
    } finally {
        if (connection) {
            try {
                await connection.end();
                console.log('Conexão fechada.');
            } catch (err) {
                console.error('Erro ao fechar conexão:', err);
            }
        }
    }
}

module.exports = { executeQuery };