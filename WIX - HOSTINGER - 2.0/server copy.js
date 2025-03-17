const express = require('express');
require('dotenv').config();
const { executeQuery } = require('./queryExecutor');
const mysql = require('mysql2');

const app = express();
app.use(express.json());

// Middleware para validar a chave secreta
function validateSecretKey(req, res, next) {
    const providedKey = req.body?.requestContext?.settings?.secretKey;

    if (!providedKey || providedKey !== process.env.SECRET_KEY) {
        console.warn('Tentativa de acesso não autorizado.');
        return res.status(403).json({ error: 'Acesso não autorizado' });
    }

    next();
}

// Função para validar consultas SQL
function validateQuery(sql) {
    if (!sql || typeof sql !== 'string') {
        throw new Error('Consulta SQL inválida');
    }

    // Verifica se a consulta contém palavras-chave proibidas
    const forbiddenKeywords = ['DROP', 'DELETE', 'ALTER', 'TRUNCATE'];
    const upperCaseSql = sql.toUpperCase();

    for (const keyword of forbiddenKeywords) {
        if (upperCaseSql.includes(keyword)) {
            throw new Error(`Uso da palavra-chave '${keyword}' não permitido`);
        }
    }
}

app.post('/api/query/provision', validateSecretKey, (req, res) => {
    try {
        const installationId = req.body?.requestContext?.installationId;

        if (!installationId) {
            return res.status(400).json({ error: 'installationId não fornecido' });
        }

        console.log(`Provisionamento realizado para installationId: ${installationId}`);

        res.status(200).json({
            status: 'success',
            instanceId: installationId,
            message: 'Provisionamento concluído com sucesso'
        });
    } catch (err) {
        console.error('Erro no provisionamento:', err.message);
        res.status(500).json({ error: 'Erro interno do servidor' });
    }
});

// Endpoint para QUERY (GET)
app.get('/api/query', validateSecretKey, async (req, res) => {
    try {
        const sql = req.body.sql;

        if (!sql) {
            return res.status(400).json({ error: 'Consulta SQL inválida' });
        }

        validateQuery(sql);

        const result = await executeQuery(sql);
        res.json({ rows: result });
    } catch (err) {
        console.error('Erro ao executar consulta GET:', err.message);
        res.status(400).json({ error: err.message });
    }
});

// Endpoint para UPDATE/DELETE (POST)
app.post('/api/update', validateSecretKey, async (req, res) => {
    try {
        const sql = req.body.sql;

        if (!sql) {
            return res.status(400).json({ error: 'Consulta SQL inválida' });
        }

        validateQuery(sql);

        await executeQuery(sql);
        res.json({ message: 'Operação realizada com sucesso' });
    } catch (err) {
        console.error('Erro ao executar consulta POST:', err.message);
        res.status(400).json({ error: err.message });
    }
});

// Endpoint para encontrar schemas específicos
app.post('/api/query/schemas/find', validateSecretKey, async (req, res) => {
    try {
        const { schemaIds } = req.body;

        // Validação dos parâmetros
        if (!Array.isArray(schemaIds) || schemaIds.length === 0) {
            return res.status(400).json({ error: 'Lista de schemaIds não fornecida ou vazia' });
        }

        // Constrói o objeto de resposta
        const schemas = [];

        for (const table of schemaIds) {
            const escapedTable = mysql.escapeId(table);
            const sqlColumns = `DESCRIBE ${escapedTable}`;
            const columnsResult = await executeQuery(sqlColumns);

            // Se a tabela não existir, ignorar e continuar
            if (!Array.isArray(columnsResult)) {
                console.warn(`Tabela ${table} não encontrada ou resultados inválidos.`);
                continue;
            }

            // Mapeia os campos para o formato esperado pelo Wix
            const fields = {
                _id: {
                    displayName: "_id",
                    type: "text",
                    queryOperators: [
                        "eq", "lt", "gt", "hasSome", "and", "lte", "gte", "or", "not", "ne", "startsWith", "endsWith"
                    ]
                },
                _owner: {
                    displayName: "_owner",
                    type: "text",
                    queryOperators: [
                        "eq", "lt", "gt", "hasSome", "and", "lte", "gte", "or", "not", "ne", "startsWith", "endsWith"
                    ]
                }
            };

            for (const column of columnsResult) {
                const fieldName = column.Field || '';
                const fieldType = mapMySQLTypeToWixType(column.Type || '');
                const isRequired = column.Null === 'NO'; // Campo obrigatório se Null for "NO"

                fields[fieldName] = {
                    displayName: fieldName,
                    type: fieldType,
                    queryOperators: [
                        "eq", "lt", "gt", "hasSome", "and", "lte", "gte", "or", "not", "ne", "startsWith", "endsWith"
                    ],
                    required: isRequired
                };
            }

            // Adiciona o schema ao array de schemas
            schemas.push({
                displayName: table,
                id: table,
                allowedOperations: ["get", "find", "count", "update", "insert", "remove"],
                maxPageSize: 50,
                ttl: 3600,
                fields: fields
            });
        }

        // Retorna a resposta no formato esperado pelo Wix
        res.status(200).json({ schemas });
    } catch (err) {
        console.error('Erro ao encontrar schemas:', err.message);
        res.status(500).json({ error: 'Erro interno do servidor' });
    }
});

// Função auxiliar para mapear tipos MySQL para tipos Wix
function mapMySQLTypeToWixType(mysqlType) {
    if (mysqlType.includes('int') || mysqlType.includes('float') || mysqlType.includes('double')) {
        return 'number';
    } else if (mysqlType.includes('date') || mysqlType.includes('timestamp')) {
        return 'datetime';
    } else {
        return 'text';
    }
}

app.post('/api/items/find', validateSecretKey, async (req, res) => {
    try {
        const { table, filter, limit, offset } = req.body;
        const escapedTable = mysql.escapeId(table);

        let sql = `SELECT * FROM ${escapedTable}`;
        const params = [];

        if (filter) {
            const conditions = Object.entries(filter)
                .map(([key, value]) => `${mysql.escapeId(key)} = ?`)
                .join(' AND ');
            sql += ` WHERE ${conditions}`;
            params.push(...Object.values(filter));
        }

        if (limit) {
            sql += ` LIMIT ?`;
            params.push(limit);
        }

        if (offset) {
            sql += ` OFFSET ?`;
            params.push(offset);
        }

        const result = await executeQuery(sql, params);
        res.status(200).json({ items: result });
    } catch (err) {
        console.error('Erro ao encontrar itens:', err.message);
        res.status(500).json({ error: 'Erro interno do servidor' });
    }
});

app.post('/api/query/data/count', validateSecretKey, async (req, res) => {
    try {
        // Extrai os dados da requisição
        const { collectionName, filter } = req.body;

        // Valida se o nome da coleção foi fornecido
        if (!collectionName) {
            return res.status(400).json({ error: 'Nome da coleção é obrigatório' });
        }

        // Escapa o nome da tabela para evitar SQL injection
        const escapedTable = mysql.escapeId(collectionName);

        // Monta a consulta SQL básica
        let sql = `SELECT COUNT(*) AS totalCount FROM ${escapedTable}`;
        const params = [];

        // Adiciona condições de filtro, se existirem
        if (filter && Object.keys(filter).length > 0) {
            const conditions = Object.entries(filter)
                .map(([key, value]) => `${mysql.escapeId(key)} = ?`)
                .join(' AND ');
            sql += ` WHERE ${conditions}`;
            params.push(...Object.values(filter));
        }

        // Executa a consulta no banco de dados
        const result = await executeQuery(sql, params);

        // Retorna o total de registros no formato esperado pelo Wix
        res.status(200).json({ totalCount: result[0].totalCount });
    } catch (err) {
        console.error('Erro ao contar itens:', err.message);
        res.status(500).json({ error: 'Erro interno do servidor' });
    }
});

app.get('/api/items/:table/:id', validateSecretKey, async (req, res) => {
    try {
        const { table, id } = req.params;
        const escapedTable = mysql.escapeId(table);
        const sql = `SELECT * FROM ${escapedTable} WHERE _id = ?`;
        const result = await executeQuery(sql, [id]);

        if (!result || result.length === 0) {
            return res.status(404).json({ error: 'Item não encontrado' });
        }

        res.status(200).json({ item: result[0] });
    } catch (err) {
        console.error('Erro ao obter item:', err.message);
        res.status(500).json({ error: 'Erro interno do servidor' });
    }
});

app.post('/api/query/data/insert', validateSecretKey, async (req, res) => {
    try {
        // Extrai os dados da requisição
        const { collectionName, item, data } = req.body;

        // Valida se o nome da coleção foi fornecido
        if (!collectionName || typeof collectionName !== 'string') {
            return res.status(400).json({ error: 'Nome da coleção é obrigatório e deve ser uma string.' });
        }

        // Determina qual campo usar para os dados (prioriza "item" sobre "data")
        const payload = item || data;

        // Valida se os dados a serem inseridos foram fornecidos
        if (!payload || typeof payload !== 'object' || Object.keys(payload).length === 0) {
            return res.status(400).json({ error: 'Os dados a serem inseridos são obrigatórios e devem ser um objeto não vazio.' });
        }

        // Escapa o nome da tabela para evitar SQL injection
        const escapedTable = mysql.escapeId(collectionName);

        // Prepara os campos e valores para a inserção
        const fields = [];
        const placeholders = [];
        const values = [];

        for (const [key, value] of Object.entries(payload)) {
            fields.push(mysql.escapeId(key)); // Escapa o nome do campo
            placeholders.push('?'); // Placeholder para o valor
            values.push(value); // Valor a ser inserido
        }

        // Monta a consulta SQL de inserção
        const sql = `INSERT INTO ${escapedTable} (${fields.join(', ')}) VALUES (${placeholders.join(', ')})`;

        // Executa a consulta SQL
        const result = await executeQuery(sql, values);

        // Retorna o ID do item inserido (se aplicável)
        const insertedId = result.insertId || null;

        // Retorna a resposta no formato esperado pelo Wix
        res.status(200).json({
            _id: insertedId ? insertedId.toString() : null,
            message: 'Item inserido com sucesso.'
        });
    } catch (err) {
        console.error('Erro ao inserir item:', err.message);
        res.status(500).json({ error: 'Erro interno do servidor' });
    }
});

app.post('/api/query/data/find', validateSecretKey, async (req, res) => {
    try {
        const { collectionName, filter, sort, skip, limit, returnTotalCount } = req.body;

        if (!collectionName) {
            return res.status(400).json({ error: 'Nome da coleção é obrigatório' });
        }

        const escapedTable = mysql.escapeId(collectionName);
        let sql = `SELECT * FROM ${escapedTable}`;
        const params = [];

        if (filter && Object.keys(filter).length > 0) {
            const { operator, fieldName, value } = filter;

            if (!['$hasSome', '$eq'].includes(operator)) {
                return res.status(400).json({ error: 'Operador de filtro inválido' });
            }

            if (operator === '$hasSome') {
                if (!Array.isArray(value)) {
                    return res.status(400).json({ error: 'O valor do filtro deve ser um array para o operador $hasSome' });
                }
                const placeholders = value.map(() => '?').join(',');
                sql += ` WHERE ${mysql.escapeId(fieldName)} IN (${placeholders})`;
                params.push(...value);
            } else if (operator === '$eq') {
                sql += ` WHERE ${mysql.escapeId(fieldName)} = ?`;
                params.push(value);
            }
        }

        if (sort && sort.length > 0) {
            const sortConditions = sort.map(({ fieldName, order }) => {
                const escapedField = mysql.escapeId(fieldName);
                return `${escapedField} ${order === 'ASC' ? 'ASC' : 'DESC'}`;
            }).join(', ');
            sql += ` ORDER BY ${sortConditions}`;
        }

        if (typeof skip === 'number' && typeof limit === 'number') {
            sql += ` LIMIT ?, ?`;
            params.push(skip, limit);
        }

        console.log('SQL Query:', sql);
        console.log('Params:', params);

        const items = await executeQuery(sql, params);

        if (!Array.isArray(items) || items.length === 0) {
            return res.status(200).json({ items: [] });
        }

        let totalCount = null;
        if (returnTotalCount) {
            const countSql = `SELECT COUNT(*) AS totalCount FROM ${escapedTable}`;
            const countResult = await executeQuery(countSql, []);
            totalCount = countResult[0].totalCount;
        }

        const formattedItems = items.map((item) => {
            const formattedItem = { ...item };
            if (formattedItem.date_added instanceof Date) {
                formattedItem.date_added = { "$date": formattedItem.date_added.toISOString() };
            } else {
                delete formattedItem.date_added;
            }
            return formattedItem;
        });

        const response = { items: formattedItems };
        if (returnTotalCount) {
            response.totalCount = totalCount;
        }

        res.status(200).json(response);
    } catch (err) {
        console.error('Erro ao buscar itens:', err.message, err.stack);
        res.status(500).json({ error: 'Erro interno do servidor', details: err.message });
    }
});

app.post('/api/query/data/update', validateSecretKey, async (req, res) => {
    try {
        const { collectionName, item } = req.body;

        // Valida se o nome da coleção foi fornecido
        if (!collectionName) {
            return res.status(400).json({ error: 'Nome da coleção é obrigatório' });
        }

        // Valida se o item contém os dados necessários
        if (!item || !item._id) {
            return res.status(400).json({ error: 'Item inválido ou ID ausente' });
        }

        // Escapa o nome da tabela para evitar SQL Injection
        const escapedTable = mysql.escapeId(collectionName);

        // Monta a consulta SQL para atualização
        const updateFields = Object.keys(item)
            .filter(key => key !== '_id') // Ignora o campo _id, pois ele é usado na cláusula WHERE
            .map(key => `${mysql.escapeId(key)} = ?`)
            .join(', ');

        if (!updateFields) {
            return res.status(400).json({ error: 'Nenhum campo para atualizar' });
        }

        const sql = `UPDATE ${escapedTable} SET ${updateFields} WHERE _id = ?`;
        const params = [...Object.values(item).filter((_, index) => index !== Object.keys(item).indexOf('_id')), item._id];

        console.log('SQL Query:', sql);
        console.log('Params:', params);

        // Executa a consulta de atualização
        const result = await executeQuery(sql, params);

        // Verifica se algum registro foi afetado
        if (result.affectedRows === 0) {
            return res.status(404).json({ error: 'Registro não encontrado' });
        }

        // Retorna a resposta de sucesso
        res.status(200).json({ message: 'Registro atualizado com sucesso', affectedRows: result.affectedRows });
    } catch (err) {
        console.error('Erro ao atualizar registro:', err.message, err.stack);
        res.status(500).json({ error: 'Erro interno do servidor', details: err.message });
    }
});

app.post('/api/query/data/remove', validateSecretKey, async (req, res) => {
    try {
        const { collectionName, itemId } = req.body;

        // Valida se o nome da coleção foi fornecido
        if (!collectionName) {
            return res.status(400).json({ error: 'Nome da coleção é obrigatório' });
        }

        // Valida se o ID do item foi fornecido
        if (!itemId) {
            return res.status(400).json({ error: 'ID do item é obrigatório' });
        }

        // Escapa o nome da tabela para evitar SQL Injection
        const escapedTable = mysql.escapeId(collectionName);

        // Monta a consulta SQL para remoção
        const sql = `DELETE FROM ${escapedTable} WHERE _id = ?`;
        const params = [itemId];

        console.log('SQL Query:', sql);
        console.log('Params:', params);

        // Executa a consulta de remoção
        const result = await executeQuery(sql, params);

        // Verifica se algum registro foi afetado
        if (result.affectedRows === 0) {
            return res.status(404).json({ error: 'Registro não encontrado' });
        }

        // Retorna a resposta de sucesso
        res.status(200).json({ message: 'Registro removido com sucesso', affectedRows: result.affectedRows });
    } catch (err) {
        console.error('Erro ao remover registro:', err.message, err.stack);
        res.status(500).json({ error: 'Erro interno do servidor', details: err.message });
    }
});

// Endpoint para listar schemas
app.post('/api/query/schemas/list', validateSecretKey, async (req, res) => {
    try {
        // Consulta SQL para obter as tabelas no MySQL
        const sqlTables = 'SHOW TABLES';
        const tablesResult = await executeQuery(sqlTables);

        if (!tablesResult || tablesResult.length === 0) {
            return res.status(404).json({ error: 'Nenhuma tabela encontrada no banco de dados' });
        }

        // Extrai os nomes das tabelas
        const tables = tablesResult.map(row => row[`Tables_in_${process.env.DB_NAME}`]);

        // Constrói o objeto de resposta
        const schemas = [];

        for (const table of tables) {
            const escapedTable = mysql.escapeId(table);
            const sqlColumns = `DESCRIBE ${escapedTable}`;
            const columnsResult = await executeQuery(sqlColumns);

            // Mapeia os campos existentes na tabela para o formato esperado pelo Wix
            const fields = {};

            for (const column of columnsResult) {
                const fieldName = column.Field || '';
                const fieldType = mapMySQLTypeToWixType(column.Type || '');
                const isRequired = column.Null === 'NO'; // Campo obrigatório se Null for "NO"
                const isUnique = column.Key === 'PRI' || column.Key === 'UNI'; // Campo único se Key for "PRI" ou "UNI"

                // Define operadores de consulta com base no tipo do campo
                let queryOperators = [];
                if (fieldType === 'text') {
                    queryOperators = ["eq", "lt", "gt", "hasSome", "and", "lte", "gte", "or", "not", "ne", "startsWith", "endsWith"];
                } else if (fieldType === 'number') {
                    queryOperators = ["eq", "lt", "gt", "hasSome", "and", "lte", "gte", "or", "not", "ne"];
                } else if (fieldType === 'datetime') {
                    queryOperators = ["eq", "lt", "gt", "hasSome", "and", "lte", "gte", "or", "not", "ne"];
                } else {
                    queryOperators = ["eq", "lt", "gt", "hasSome", "and", "lte", "gte", "or", "not", "ne"];
                }

                // Renomeia o campo ID para _id se for uma chave primária
                const finalFieldName = column.Key === 'PRI' ? '_id' : fieldName;

                // Adiciona o campo ao objeto fields
                fields[finalFieldName] = {
                    displayName: fieldName,
                    type: fieldType,
                    required: isRequired,
                    unique: isUnique,
                    queryOperators: queryOperators
                };
            }

            // Adiciona o schema ao array de schemas
            schemas.push({
                displayName: table,
                id: table,
                allowedOperations: [
                    "get", "find", "count", "update", "insert", "remove"
                ],
                maxPageSize: 50,
                ttl: 3600,
                fields: fields
            });
        }

        // Retorna a resposta no formato esperado pelo Wix
        res.status(200).json({ schemas });
    } catch (err) {
        console.error('Erro ao listar schemas:', err.message);
        res.status(500).json({ error: 'Erro interno do servidor' });
    }
});

// Função para mapear tipos MySQL para tipos Wix
function mapMySQLTypeToWixType(mysqlType) {
    mysqlType = mysqlType.toLowerCase();

    if (mysqlType.includes('int')) {
        return 'number';
    } else if (mysqlType.includes('varchar') || mysqlType.includes('text') || mysqlType.includes('char')) {
        return 'text'; // Alterado para "text" em vez de "string"
    } else if (mysqlType.includes('date') || mysqlType.includes('time') || mysqlType.includes('datetime')) {
        return 'datetime';
    } else if (mysqlType.includes('float') || mysqlType.includes('double') || mysqlType.includes('decimal')) {
        return 'number';
    } else if (mysqlType.includes('bool')) {
        return 'boolean';
    } else {
        return 'any'; // Tipo padrão caso não seja reconhecido
    }
}



// Inicializa o servidor
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Servidor rodando na porta ${PORT}`));