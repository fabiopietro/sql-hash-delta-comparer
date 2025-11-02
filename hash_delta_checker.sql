SET NOCOUNT ON; 

-- Variaveis para parametrizacao do processo 
DECLARE @TabelaProducao             SYSNAME         = N'tbProducao'; 
DECLARE @TabelaHomologacao          SYSNAME         = N'tbHomologacao'; 
DECLARE @ColunasChave               NVARCHAR(MAX)   = N'cod_simulacao,modelo'; 
DECLARE @ColunasExcecao             NVARCHAR(MAX)   = N'desconto, data_parametro'; -- Diferença prevista devido a ajuste de regra 

DECLARE @Limitador                  NVARCHAR(MAX)   = N'data_parametro'; 
DECLARE @VerificaCase               BIT = 1                 -- Verificacao de maiuscula/minuscula 
DECLARE @Separador                  CHAR(1) = CHAR(30);     -- Caracter Invisivel (Unicode para separador de registros) 

-- Variaveis de trabalho 
DECLARE @NomeDaColuna               SYSNAME; 
DECLARE @DataType                   SYSNAME; 
DECLARE @Colunas                    NVARCHAR(MAX)   = N''; 
DECLARE @SQL                        NVARCHAR(MAX); 
DECLARE @Debug                      BIT = 0                -- Se Ativo serão exibidos as queries dinamicas


-- Variaveis para testes de quantidades/delta 
DECLARE @qtdProducao                BIGINT 
DECLARE @qtdHomolog                 BIGINT 
DECLARE @qtdDeltaIncluidos          BIGINT 
DECLARE @qtdDeltaExcluidos          BIGINT 
DECLARE @qtdDeltaHash               BIGINT 

-- Limpa execução anterior 
DROP TABLE IF EXISTS #HashProducao 
DROP TABLE IF EXISTS #HashHomolog 
DROP TABLE IF EXISTS #Auditoria 


-- Cria tabela de Hash da produção 
CREATE TABLE #HashProducao ( 
                              Chave VARCHAR(200)
                            , Hash VARCHAR(200) 
                            ); 

-- Cria tabela de Hash de homologacao 
CREATE TABLE #HashHomolog    ( 
                              Chave VARCHAR(200)
                            , Hash VARCHAR(200) 
                            ); 

-- Cria tabela de Hash de homologacao 
CREATE TABLE #Auditoria    ( 
                              Chave        VARCHAR(200)
                            , Mensagem  VARCHAR(300) 
                            ); 

-- Como as colunas são informadas manualmente pode haver espaco extra
SET @ColunasChave   = REPLACE(@ColunasChave, ' ', ''); 
SET @ColunasChave   = REPLACE(@ColunasChave, ',', ',''' + @Separador + ''','); 
SET @ColunasExcecao = REPLACE(@ColunasExcecao, ' ', '')

-- Previne espaco no nome da coluna delimitador S
SET @Limitador      = QUOTENAME(@Limitador) 

--Cria cursor para ler o nome de todas as colunas da tabela no ambiente de producao D
DECLARE cursor1 CURSOR FOR  SELECT c.name ,t.name AS typ 
                            FROM sys.columns    c 
                            JOIN sys.types      t ON t.user_type_id = c.user_type_id 
                            JOIN sys.objects    o ON o.object_id = c.object_id 
                            WHERE o.name = @TabelaProducao 
                              AND o.type = 'U' 
                              AND ( 
                                     -- Nao teve coluna excecao
                                    LEN(@ColunasExcecao) = 0 OR             
                                    --Previne excluir coluna por parte do nome
                                    CHARINDEX(',' + c.name + ',', ',' +@ColunasExcecao + ',')= 0 
                                  ) 

OPEN cursor1; 


FETCH NEXT FROM cursor1 INTO @NomeDaColuna ,@DataType;

WHILE @@FETCH_STATUS = 0 
BEGIN 

DECLARE @expr NVARCHAR(MAX); 
-- Preve que exista espaco dentro do nome da coluna 

SET @NomeDaColuna = QUOTENAME(@NomeDaColuna) 

    -- expressão padronizada por tipo para HASH/DETAIL 
    IF @DataType IN ('date','datetime','smalldatetime','datetime2','datetimeoffset','time') 
        SET @expr = N'ISNULL(CONVERT(CHAR(33), ' + @NomeDaColuna + ', 126), '''')'; 
    ELSE IF @DataType IN ('uniqueidentifier') 
        SET @expr = N'ISNULL(CONVERT(CHAR(36), ' + @NomeDaColuna +'), '''')'; 
    ELSE IF @DataType IN ('int','bigint','smallint','tinyint','bit','decimal','numeric','money','smallmoney','float','real')
        SET @expr = N'ISNULL(CONVERT(VARCHAR(100), '+ @NomeDaColuna +'), '''')'; 
    ELSE 
        SET @expr = N'ISNULL(CAST('+ @NomeDaColuna +' AS NVARCHAR(MAX)), '''')'; 


    IF @VerificaCase = 0 
        SET @Colunas += REPLICATE(' ', 57) + @expr + ',' + CHAR(13);
    ELSE 
        SET @Colunas += REPLICATE(' ', 57) + N'UPPER(' + @expr + '),'  + CHAR(13);

    -- Carrega proxima linha do cursor 
    FETCH NEXT FROM cursor1 INTO @NomeDaColuna ,@DataType; 
END 

-- Fecha e limpa o cursor 
CLOSE cursor1; 
DEALLOCATE cursor1; 

-- Retira ultima virgula 
SET @Colunas            = LEFT(@Colunas, LEN(@Colunas) - 2) 



------------------------------------------------------------
-- Cria tabelas Hash
------------------------------------------------------------
-- Previne espaco no nome das tabelas 
SET @TabelaProducao        = QUOTENAME(@TabelaProducao) 
SET @TabelaHomologacao    = QUOTENAME(@TabelaHomologacao) 

-- Build da SQL da tabelade producao 
SET @SQL = N'
INSERT INTO #HashProducao 
SELECT CONCAT(' + @ColunasChave + N') AS CHAVE, 
CONVERT(VARCHAR(64), HASHBYTES(''SHA2_256'', CONCAT(' + CHAR(13) + @Colunas + N')),2) AS Hash 
FROM ' + @TabelaProducao + N';'; 


-- Se o limitador foi informado refaz o SQL pra considerar isso (Inclui WHERE Coluna...) 
IF LEN(@Limitador) > 0 
BEGIN 
    SET @SQL = LEFT(@SQL, LEN(@SQL) - 1); 
    SET @SQL += N' WHERE ' + @Limitador + N' = (SELECT MAX(' + @Limitador + N') FROM ' + @TabelaProducao + N');'; 
END 


IF @Debug = 1 
BEGIN 
    PRINT '------------------------------------------------------------------------------'
    PRINT 'Query da tabela de produção'
    PRINT '------------------------------------------------------------------------------'
    PRINT CHAR(13) + @SQL + CHAR(13)
END

-- Carrega a tabela Hash de homologacao
EXEC (@SQL)  


-- Build da SQL da tabela de homologação 
SET @SQL = N'INSERT INTO #HashHomolog 
SELECT CONCAT(' + @ColunasChave + N') AS CHAVE, 
CONVERT(VARCHAR(64), HASHBYTES(''SHA2_256'', CONCAT(' + CHAR(13) + @Colunas + N')),2) AS Hash 
FROM ' + @TabelaHomologacao + ';'; 


IF @Debug = 1 
BEGIN 
    PRINT '------------------------------------------------------------------------------'
    PRINT 'Query da tabela de homologacao'
    PRINT '------------------------------------------------------------------------------'
    PRINT CHAR(13) + @SQL + CHAR(13)

END

-- Carrega a tabela Hash de homologacao
EXEC (@SQL) 

------------------------------------------------------------
-- Calcula Deltas
------------------------------------------------------------

PRINT '------------------------------------------------------------------------------'
PRINT 'Apuração dos Delta'
PRINT '------------------------------------------------------------------------------'


SELECT @qtdProducao = COUNT_BIG(*)  FROM #HashProducao 
SELECT @qtdHomolog  = COUNT_BIG(*)  FROM #HashHomolog 


-- Carrega na tabela de auditoria os registros que foram excluidos
INSERT INTO #Auditoria
SELECT Chave , 'Registro excluido'
FROM (
    SELECT Chave 
    FROM #HashProducao 
    EXCEPT 
    SELECT Chave 
    FROM #HashHomolog 
 ) AS Delta;
 

-- Carrega na tabela de auditoria os registros que foram excluidos
INSERT INTO #Auditoria
SELECT Chave, 'Registro novo'
FROM (
    SELECT Chave 
    FROM #HashHomolog
    EXCEPT 
    SELECT Chave 
    FROM #HashProducao 
) AS Delta;

-- Carrega na tabela de auditoria os registros que foram excluidos    
INSERT INTO #Auditoria
SELECT  A.Chave
      ,'Chave identica, mas com diferença imprevista'
FROM #HashProducao A 
INNER JOIN #HashHomolog B    ON B.Chave = A.Chave 
                            AND B.Hash <> A.Hash; 


------------------------------------------------------------
-- Calcula Deltas
------------------------------------------------------------
-- Delta Excluidos(quantidade pode estar igual, mas ter registro ausente compensado por uma inclusao) 
SELECT @qtdDeltaExcluidos   = COUNT_BIG(*)  FROM  #Auditoria WHERE Mensagem = 'Registro excluido'

-- Delta Novos(quantidade pode estar igual, mas ter registro novo devido compensado por uma exclusão) 
SELECT @qtdDeltaIncluidos   = COUNT_BIG(*)  FROM  #Auditoria WHERE Mensagem = 'Registro novo'

-- Calcula quantidade de registros com chave identica, mas Hash diferente (Imprevisto) 
SELECT @qtdDeltaHash        = COUNT_BIG(*)  FROM  #Auditoria WHERE Mensagem = 'Chave identica, mas com diferença imprevista'


--Verifica se quantidades bateu 
IF @qtdProducao <> @qtdHomolog 
    PRINT 'Divergencia na quantidade: Producao com ' + FORMAT(@qtdProducao, 'N0', 'pt-BR') + ' registros ' + CHAR(13) + 
          '                           Homolog com ' + FORMAT(@qtdHomolog, 'N0', 'pt-BR') + ' registros ' 
ELSE 
    PRINT 'Quantidade de registros identica em ambas as tabelas: ' + FORMAT(@qtdProducao, 'N0', 'pt-BR') + ' registros ' 
    

--Imprime a quantidade de cada Delta 
IF @qtdDeltaExcluidos > 0     
    PRINT 'Foram excluidos '   + FORMAT(@qtdDeltaExcluidos, 'N0', 'pt-BR') + ' registros' 

IF @qtdDeltaIncluidos > 0     
    PRINT 'Foram incluidos '   + FORMAT(@qtdDeltaIncluidos, 'N0', 'pt-BR') + ' novos registros' 
                         
IF @qtdDeltaHash > 0 
    PRINT 'Foram encontrados ' + FORMAT(@qtdDeltaHash, 'N0', 'pt-BR') + ' registros com chave identica, mas com diferença imprevista' 



PRINT '------------------------------------------------------------------------------'


SELECT * FROM #Auditoria
