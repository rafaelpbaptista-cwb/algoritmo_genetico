import traceback
import smtplib
import enum
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from infra_copel import config

class FormatoAnexoEmail(enum.Enum):
    """
    Enum para descriminar o formato do anexo do email

    Parameters
    ----------
    enum : enum.Enum
        Enum
    """
    EXCEL = enum.auto(),
    CSV = enum.auto()

def send_email(para: str,
               assunto: str,
               corpo_email: str,
               anexos: list[dict] = None,
               formato_anexo: FormatoAnexoEmail = FormatoAnexoEmail.EXCEL):
    """
    Envio de email.
    ATENÇÃO: o servidor smtp exchangeprd.copel.nt não envia email para as chaves c0xxxxx

    Parameters
    ----------
    para : list
        Lista de destinatários
    assunto : str
        Assunto do email
    corpo_email : str
        Corpo do email
    anexos : list[dict], optional
        Lista de anexos contendo um dicionário contendo nome (nome do arquivo) e
        df (dataframe a ser exportado), by default None
    formato_anexo : FormatoAnexoEmail, optional
        Detalha qual o formato do arquivo a ser anexado no email, by default FormatoAnexoEmail.EXCEL
    """

    remetente = 'cppc_automacao@copel.com'

    multipart = MIMEMultipart()
    multipart['From'] = remetente
    multipart['To'] = para
    multipart['Subject'] = assunto

    if anexos:
        for anexo in anexos:
            if formato_anexo == FormatoAnexoEmail.EXCEL:
                attachment = MIMEApplication(anexo['df'].to_excel())
                attachment['Content-Disposition'] = 'attachment; filename="{}"'.format(f"{anexo['nome']}.xlsx")
            else:
                attachment = MIMEApplication(anexo['df'].to_csv(sep=';', decimal=','))
                attachment['Content-Disposition'] = 'attachment; filename="{}"'.format(f"{anexo['nome']}.csv")

            multipart.attach(attachment)

    multipart.attach(MIMEText(corpo_email, 'html'))

    smtp = smtplib.SMTP('exchangeprd.copel.nt')
    smtp.sendmail(remetente, para, multipart.as_string())
    smtp.quit()

def notificacao_erro_airflow(context):
    """
    Método que realiza a notificação de erro caso alguma task da DAG lance alguma exceção.

    Parameters
    ----------
    context : airflow.utils.context.Context
        Contexto de execução da DAG
    """
    dag_id = context['dag'].dag_id
    task_id = context['ti'].task_id
    assunto = f"Erro na execução da DAG '{dag_id}' task '{task_id}'"
    exception_lines = traceback.format_exception(context['exception'])
    exception_html = ''.join([line.replace('\n', '<br \>') for line in exception_lines])

    logging.info('Enviando notificação de erro por email')

    send_email(para=config.get_variable('email_notificacao'),
               assunto=assunto,
               corpo_email=exception_html)
