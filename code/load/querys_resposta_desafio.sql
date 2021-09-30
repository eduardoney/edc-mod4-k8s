--Qual era o número de alunos de cursos de Filosofia inscritos no ENADE 2017?
SELECT count(1) as qtd
from `gcp-estudos-engdados.bootedc.microdados_enade` en
where en.CO_GRUPO in (3201,3202)

--Qual é o número de alunos dos cursos de Filosofia do sexo Masculino?
SELECT count(1) as qtd
, TP_SEXO
from `gcp-estudos-engdados.bootedc.microdados_enade` en
where en.CO_GRUPO in (3201,3202)
group by TP_SEXO

--Qual é o código de UF que possui a maior média de nota geral entre os alunos dos cursos de Filosofia?
SELECT avg(NT_GER) as media_NT_GER
, CO_UF_CURSO
from `gcp-estudos-engdados.bootedc.microdados_enade` en
where en.CO_GRUPO in (3201,3202)
group by CO_UF_CURSO
order by media_NT_GER desc

--Qual é a diferença das médias de idade (arredondado para 2 dígitos decimais) entre os alunos do sexo masculino e feminino, dos cursos de filosofia?
WITH medias as (
SELECT round(avg(if(TP_SEXO='F',NU_IDADE,0)),2) as media_F
    ,  round(avg(if(TP_SEXO='M',NU_IDADE,0)),2) as media_M
from `gcp-estudos-engdados.bootedc.microdados_enade` en
where en.CO_GRUPO in (3201,3202)
group by TP_SEXO
)
SELECT round(sum(media_F) - sum(media_M),2) as diferenca_media
from medias

--Qual é a maior nota geral entre alunos de filosofia do sexo feminino?
SELECT round(max(NT_GER),1) as max_NT_GER
from `gcp-estudos-engdados.bootedc.microdados_enade` en
where en.CO_GRUPO in (3201,3202)
and TP_SEXO = 'F'

--Qual é a média da nota geral para alunos de filosofia, cujo curso está no estado (UF) de código 43?
SELECT round(avg(NT_GER),2) as media_NT_GER
from `gcp-estudos-engdados.bootedc.microdados_enade` en
where en.CO_GRUPO in (3201,3202)
AND CO_UF_CURSO = 43

--Qual é a média da nota geral para alunos de filosofia do sexo feminino, cujo curso está no estado (UF) de código 11?
SELECT round(avg(NT_GER),2) as media_NT_GER
from `gcp-estudos-engdados.bootedc.microdados_enade` en
where en.CO_GRUPO in (3201,3202)
AND CO_UF_CURSO = 11
AND TP_SEXO = 'F'

--Qual é o código do estado brasileiro que possui a maior média de idade de alunos de filosofia?
SELECT round(avg(NU_IDADE),2) as media_NU_IDADE
, CO_UF_CURSO
from `gcp-estudos-engdados.bootedc.microdados_enade` en
where en.CO_GRUPO in (3201,3202)
group by CO_UF_CURSO
order by media_NU_IDADE desc

