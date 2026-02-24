import pyodbc
import pandas as pd
import requests
import json
import time

# CONFIGURACIÓN DE CONEXIÓN
conn_str = (
    "Driver={SQL Server};"
    "Server=tangoserver;"
    "Database=crisa_real1;"
    "UID=Axoft;"
    "PWD=Axoft;"
)

# URL DE TU API EN REPLIT
REPL_URL = "https://551f46a0-9017-4b9c-b45c-7fa58ca01f34-00-3437h96u7obki.worf.replit.dev/sync"

# Para producción usa:
# REPL_URL = "https://crisa-reposicion.replit.app/sync"


def get_data():
    while True:
        try:
            conn = pyodbc.connect(conn_str)
            print("Conectado a SQL Server...")
            
            # QUERY DE SALDO (STOCK) - TU QUERY ORIGINAL
            query_saldo = """
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
            SET DATEFORMAT DMY 
            SET DATEFIRST 7 
            SET DEADLOCK_PRIORITY -8;
            SELECT 
                CTA_ARTICULO.COD_CTA_ARTICULO AS [Cód. Artículo] ,
                CTA_ARTICULO.DESC_CTA_ARTICULO AS [Artículo] ,
                CTA_ARTICULO.SINONIMO AS [Sinónimo] ,
                CTA_DEPOSITO.COD_CTA_DEPOSITO AS [Cód. Depósito] ,
                SUCURSAL.NRO_SUCURSAL AS [Nro. Sucursal] ,
                SUCURSAL.DESC_SUCURSAL AS [Sucursal] ,
                CTA_DEPOSITO.DESC_CTA_DEPOSITO AS [Depósito] ,
                 (CASE  CTA_ARTICULO.BASE when '' then  CTA_ARTICULO.COD_ARTICULO ELSE  CTA_ARTICULO.BASE end) AS [Cód. base / artículo] ,
                (CASE  CTA_ARTICULO.BASE when '' then  CTA_ARTICULO.DESC_CTA_ARTICULO ELSE BASE.DESC_CTA_ARTICULO end) AS [Desc. Base / Artículo] ,
                CTA_ARTICULO.ESCALA_1 AS [Cód. escala 1] ,
                STA33.DESC_VALOR AS [Desc. escala 1] ,
                MEDIDA_STOCK.SIGLA_MEDIDA AS [U.M. stock] ,
                SUM(CTA_SALDO_ARTICULO_DEPOSITO.CANTIDAD_STOCK) AS [Stock 1] 
            FROM 
            CTA_SALDO_ARTICULO_DEPOSITO RIGHT JOIN (SELECT ID_CTA_ARTICULO, ID_CTA_DEPOSITO,ID_SUCURSAL,MAX(FECHA) AS [FECHA_MAX] FROM CTA_SALDO_ARTICULO_DEPOSITO GROUP BY ID_CTA_ARTICULO, ID_CTA_DEPOSITO,ID_SUCURSAL) AS ULT_SALDO ON (CTA_SALDO_ARTICULO_DEPOSITO.ID_CTA_ARTICULO = ULT_SALDO.ID_CTA_ARTICULO AND CTA_SALDO_ARTICULO_DEPOSITO. ID_CTA_DEPOSITO = ULT_SALDO. ID_CTA_DEPOSITO AND CTA_SALDO_ARTICULO_DEPOSITO.ID_SUCURSAL = ULT_SALDO.ID_SUCURSAL AND CTA_SALDO_ARTICULO_DEPOSITO.FECHA = ULT_SALDO.FECHA_MAX) 
            LEFT JOIN CTA_ARTICULO ON (CTA_SALDO_ARTICULO_DEPOSITO.ID_CTA_ARTICULO = CTA_ARTICULO.ID_CTA_ARTICULO) 
            LEFT JOIN (SELECT COD_ARTICULO, DESC_CTA_ARTICULO FROM CTA_ARTICULO WHERE USA_ESC = 'B') AS BASE ON (BASE.COD_ARTICULO = CTA_ARTICULO.BASE) 
            LEFT JOIN SUCURSAL ON (CTA_SALDO_ARTICULO_DEPOSITO.ID_SUCURSAL = SUCURSAL.ID_SUCURSAL) 
            LEFT JOIN CTA_DEPOSITO ON (CTA_SALDO_ARTICULO_DEPOSITO.ID_CTA_DEPOSITO = CTA_DEPOSITO.ID_CTA_DEPOSITO) 
            LEFT JOIN STA33 ON (CTA_ARTICULO.ESCALA_1 = STA33.COD_ESCALA  AND CTA_ARTICULO.VALOR1 = STA33.COD_VALOR) 
            LEFT JOIN STA33 AS STA33_BIS ON (CTA_ARTICULO.ESCALA_2 = STA33_BIS.COD_ESCALA  AND CTA_ARTICULO.VALOR2 = STA33_BIS.COD_VALOR) 
            LEFT JOIN CTA_ARTICULO_SUCURSAL ON (CTA_SALDO_ARTICULO_DEPOSITO.ID_CTA_ARTICULO = CTA_ARTICULO_SUCURSAL.ID_CTA_ARTICULO AND CTA_SALDO_ARTICULO_DEPOSITO.ID_SUCURSAL = CTA_ARTICULO_SUCURSAL.ID_SUCURSAL) 
            LEFT JOIN CTA_MEDIDA AS MEDIDA_STOCK ON (CTA_ARTICULO_SUCURSAL.ID_CTA_MEDIDA_STOCK = MEDIDA_STOCK.ID_CTA_MEDIDA) 
            LEFT JOIN CTA_MEDIDA AS MEDIDA_STOCK_2 ON (CTA_ARTICULO_SUCURSAL.ID_CTA_MEDIDA_STOCK_2 = MEDIDA_STOCK_2.ID_CTA_MEDIDA) 


            WHERE 

            CTA_ARTICULO.STOCK = 1

            GROUP BY 
                CTA_ARTICULO.COD_CTA_ARTICULO , CTA_ARTICULO.DESC_CTA_ARTICULO , CTA_ARTICULO.SINONIMO , CTA_DEPOSITO.COD_CTA_DEPOSITO , SUCURSAL.NRO_SUCURSAL , SUCURSAL.DESC_SUCURSAL , CTA_DEPOSITO.DESC_CTA_DEPOSITO ,  (CASE  CTA_ARTICULO.BASE when '' then  CTA_ARTICULO.COD_ARTICULO ELSE  CTA_ARTICULO.BASE end) , (CASE  CTA_ARTICULO.BASE when '' then  CTA_ARTICULO.DESC_CTA_ARTICULO ELSE BASE.DESC_CTA_ARTICULO end) , CTA_ARTICULO.ESCALA_1 , STA33.DESC_VALOR , MEDIDA_STOCK.SIGLA_MEDIDA
            """

            # QUERY DE VENTAS - TU QUERY ORIGINAL
            query_ventas = """
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
            SET DATEFORMAT DMY 
            SET DATEFIRST 7 
            SET DEADLOCK_PRIORITY -8;
            SELECT 
                CTA03.FECHA_MOV AS [Fecha] ,
                CTA02.NRO_SUCURS AS [Nro. Sucursal] ,
                SUCURSAL.DESC_SUCURSAL AS [Desc. sucursal] ,
                SUBSTRING(cta02.HORA_EMIS, 1, 2) + ' hs.' AS [Hora emisión hh] ,
                CTA02.T_COMP AS [Tipo de comprobante] ,
                CTA02.N_COMP AS [Nro. Comprobante] ,
                CTA02.COND_VTA AS [Cód. condición de venta] ,
                CTA02.COD_VENDED AS [Cód. vendedor] ,
                CASE CTA02.COD_VENDED WHEN '**' THEN 'Carga inicial' ELSE CTA_VENDEDOR.DESC_VENDEDOR END AS [Nombre Vendedor] ,
                CTA02.NRO_DE_LIS AS [Lista de precios] ,
                LISTA_COMP.NOMBRE_LIS AS [Desc. lista de precios] ,
                CTA02.COD_CLIENT AS [Cód. cliente] ,
                CASE CTA02.COD_CLIENT WHEN '000000' THEN 'OCASIONAL' ELSE CTA_CLIENTE.NOMBRE END AS [Razón social] ,
                CTA03.Cod_Articu AS [Cód. Artículo] ,
                CTA_ARTICULO.DESC_CTA_ARTICULO AS [Descripción] ,
                CTA_ARTICULO.SINONIMO AS [Sinónimo] ,
                ISNULL(FAMILIA_ART.COD_AGR,'') AS [Cód. Familia (Artículo)] ,
                FAMILIA_ART.NOM_AGR AS [Descripción Familia (Artículo)] ,
                SUM(CASE CTA03.TCOMP_IN_V  WHEN 'CC'  THEN(-1)  ELSE(1)  END  * CTA03.CANTIDAD / CASE WHEN CAN_EQUI_V = 0 THEN 1 ELSE CAN_EQUI_V END) AS [Cantidad venta] ,
                MEDIDA_STOCK.SIGLA_MEDIDA AS [U.M. stock] ,
                CASE CTA03.TCOMP_IN_V WHEN 'CC' THEN ( -1 ) ELSE ( 1 ) END * 
                        CASE 'BIMONCTE' 
                                WHEN 'BIMONCTE' THEN 
                                        CASE CTA02.MON_CTE  
                                                WHEN 1 THEN CTA03.PRECIO_NET * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) 
                                                ELSE CTA03.PRECIO_NET * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) * CTA02.COTIZ  
                                        END 
                                WHEN 'BIORIGEN' THEN 
                                        CASE CTA02.MON_CTE  
                                                WHEN 1 THEN CASE CTA02.COTIZ WHEN 0 THEN 0 ELSE CTA03.PRECIO_NET  * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) /CTA02.COTIZ END 
                                                ELSE CTA03.PRECIO_NET  * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) 
                                        END 
                                WHEN 'BICOTIZ' THEN 
                                        CASE 1 WHEN 0 THEN 0 ELSE 
                                                CASE CTA02.MON_CTE WHEN 1 THEN  CTA03.PRECIO_NET * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) 
                                                ELSE CTA03.PRECIO_NET *  (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) * CTA02.COTIZ END / 1 
                                        END 
                        END   AS [Precio c/IVA] ,
                SUM (CASE CTA03.TCOMP_IN_V WHEN 'CC' THEN (-1) ELSE (1) END *      
                        CASE  'BIMONCTE'          
                                WHEN 'BIMONCTE' THEN  
                                        CASE CTA02.MON_CTE  
                                                WHEN 1 THEN CTA03.IMP_NETO_P * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) 
                                                ELSE  CTA03.IMP_NETO_P * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) * CTA02.COTIZ 
                                        END           
                                WHEN 'BIORIGEN' THEN  
                                        CASE CTA02.MON_CTE  
                                                WHEN 1 THEN CASE CTA02.COTIZ WHEN 0 THEN 0 ELSE CTA03.IMP_NETO_P * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) / CTA02.COTIZ END 
                                                ELSE CTA03.IMP_NETO_P * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) 
                                        END           
                                WHEN 'BICOTIZ' THEN  
                                        CASE 1 WHEN 0 THEN 0 ELSE  
                                                CASE CTA02.MON_CTE  
                                                        WHEN 1 THEN CTA03.IMP_NETO_P * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) / 1 
                                                        ELSE CTA03.IMP_NETO_P * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) * CTA02.COTIZ / 1 
                                                END 
                                        END 
                        END)  AS [Imp. prop. c/IVA] ,
                ISNULL(V_TN_SALDOS_STOCK_CENTRAL.CANTIDAD_STOCK ,0) AS [Saldo stock] 
            FROM 
                CTA03 (NOLOCK)  
                INNER JOIN CTA02 (NOLOCK) ON (CTA02.N_COMP = CTA03.N_COMP AND CTA02.T_COMP = CTA03.T_COMP AND CTA03.NRO_SUCURS = CTA02.NRO_SUCURS) 
                INNER JOIN SUCURSAL (NOLOCK) ON CTA02.NRO_SUCURS = SUCURSAL.NRO_SUCURSAL
                LEFT JOIN CTA_VENDEDOR (NOLOCK) ON CTA02.COD_VENDED = CTA_VENDEDOR.COD_VENDEDOR
                LEFT JOIN GVA10 LISTA_COMP (NOLOCK) ON CTA02.NRO_DE_LIS = LISTA_COMP.NRO_DE_LIS
                LEFT JOIN CTA_CLIENTE (NOLOCK) ON CTA02.COD_CLIENT = CTA_CLIENTE.COD_CLIENTE
                LEFT JOIN CTA_ARTICULO (NOLOCK) ON CTA03.Cod_Articu = CTA_ARTICULO.COD_ARTICULO
                LEFT JOIN STA16 ON 1=1
                LEFT JOIN STA29 FAMILIA_ART (NOLOCK) ON SUBSTRING(CTA_ARTICULO.COD_ARTICULO, 1, LONG_FAM_A) = FAMILIA_ART.COD_AGR
                LEFT JOIN (SELECT * FROM CTA_MEDIDA) AS MEDIDA_STOCK ON CTA03.ID_MEDIDA_STOCK = MEDIDA_STOCK.ID_CTA_MEDIDA
                LEFT JOIN(
                        SELECT  NRO_SUCURS, T_COMP, N_COMP, SUM(IMPORTE) AS IMPORTE
                        FROM CTA04
                        WHERE (COD_IMPUES <>'') OR ( (COD_ALICUO BETWEEN 21 AND 80 ) OR COD_ALICUO = 82)
                        GROUP BY NRO_SUCURS, T_COMP, N_COMP
                ) AS IMPUESTOS ON IMPUESTOS.T_COMP= CTA02.T_COMP AND IMPUESTOS.N_COMP = CTA02.N_COMP AND IMPUESTOS.NRO_SUCURS= CTA02.NRO_SUCURS
                LEFT JOIN CTA_ARTICULO (NOLOCK) CTA_ARTICULO2 ON CTA03.COD_ARTICU = CTA_ARTICULO2.COD_ARTICULO
                LEFT JOIN SUCURSAL (NOLOCK) SUCURSAL2 ON CTA03.NRO_SUCURS = SUCURSAL2.NRO_SUCURSAL
                LEFT JOIN CTA_DEPOSITO (NOLOCK) CTA_DEPOSITO2 ON CTA03.COD_DEPOSI = CTA_DEPOSITO2.COD_CTA_DEPOSITO
                LEFT JOIN  V_TN_SALDOS_STOCK_CENTRAL ON ( CTA_ARTICULO2.ID_CTA_ARTICULO = V_TN_SALDOS_STOCK_CENTRAL.ID_CTA_ARTICULO)
            WHERE 
                CTA03.Cod_Articu NOT IN ('Art. Ajuste') 
                AND (CTA03.Cod_Articu <> '') 
                AND CTA02.T_COMP <> 'REC'
                AND ( (CTA03.FECHA_MOV BETWEEN '09/01/2024' AND '31/05/2026')) 
                AND ((isnull(CTA03.RENGL_PADR,0) = 0) OR (isnull(CTA03.INSUMO_KIT_SEPARADO,0) = 1))
            GROUP BY 
                CTA03.FECHA_MOV , CTA02.NRO_SUCURS , SUCURSAL.DESC_SUCURSAL , SUBSTRING(cta02.HORA_EMIS, 1, 2) + ' hs.' , CTA02.T_COMP , CTA02.N_COMP , CTA02.COND_VTA , CTA02.COD_VENDED , CASE CTA02.COD_VENDED WHEN '**' THEN 'Carga inicial' ELSE CTA_VENDEDOR.DESC_VENDEDOR END , CTA02.NRO_DE_LIS , LISTA_COMP.NOMBRE_LIS , CTA02.COD_CLIENT , CASE CTA02.COD_CLIENT WHEN '000000' THEN 'OCASIONAL' ELSE CTA_CLIENTE.NOMBRE END , CTA03.Cod_Articu , CTA_ARTICULO.DESC_CTA_ARTICULO , CTA_ARTICULO.SINONIMO , ISNULL(FAMILIA_ART.COD_AGR,'') , FAMILIA_ART.NOM_AGR , MEDIDA_STOCK.SIGLA_MEDIDA ,  
                CASE CTA03.TCOMP_IN_V WHEN 'CC' THEN ( -1 )ELSE ( 1 ) END * CASE 'BIMONCTE' WHEN 'BIMONCTE' THEN CASE CTA02.MON_CTE  WHEN 1 THEN CTA03.PRECIO_NET * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) ELSE  CTA03.PRECIO_NET * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) * CTA02.COTIZ  END WHEN 'BIORIGEN' THEN CASE CTA02.MON_CTE  WHEN 1 THEN CASE CTA02.COTIZ WHEN 0 THEN 0 ELSE CTA03.PRECIO_NET  * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) /CTA02.COTIZ END ELSE CTA03.PRECIO_NET  * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) END WHEN 'BICOTIZ' THEN CASE 1 WHEN 0 THEN 0 ELSE CASE CTA02.MON_CTE WHEN 1 THEN  CTA03.PRECIO_NET * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) ELSE CTA03.PRECIO_NET *  (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE ( 1 + (CTA03.PORC_IVA/100)) END) * CTA02.COTIZ END / 1 END END   , 
                ISNULL(V_TN_SALDOS_STOCK_CENTRAL.CANTIDAD_STOCK ,0)
            """

            print("Ejecutando queries...")
            df_saldo = pd.read_sql(query_saldo, conn)
            print(f"Saldos: {len(df_saldo)} registros")
            
            df_ventas = pd.read_sql(query_ventas, conn)
            print(f"Ventas: {len(df_ventas)} registros")

            # ENVIAR DATOS TAL CUAL VIENEN DE TANGO (la API hace el mapeo)
            data = {
                "saldo": df_saldo.to_dict(orient="records"),
                "ventas": df_ventas.to_dict(orient="records"),
                "precios": []
            }

            print(f"Enviando datos a {REPL_URL}...")
            response = requests.post(REPL_URL, json=data, timeout=300)
            
            if response.status_code == 200:
                result = response.json()
                print(f"✓ Sincronización exitosa!")
                print(f"  - Saldos: {result.get('registros_saldo', 0)}")
                print(f"  - Ventas: {result.get('registros_ventas', 0)}")
                print(f"  - Métricas: {result.get('registros_resultado', 0)}")
            else:
                print(f"✗ Error: {response.status_code} - {response.text}")

            conn.close()
            
            print("\nEsperando 60 segundos para próxima sincronización...")
            time.sleep(60)
            
        except Exception as e:
            print(f"✗ Error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(60)


if __name__ == "__main__":
    print("=== Bridge SQL Server -> Replit ===")
    print(f"Destino: {REPL_URL}")
    print("="*40)
    get_data()
