package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/supabase-community/supabase-go"
)

// Device é o par de identificadores de um sensor cadastrado.
//
// São dois porque a API usa cada um em um lugar: o DevEUI identifica o sensor
// no cache (chave userId:X:devEUI:Y:history) e o DevAddr é a tag pela qual o
// InfluxDB indexa as leituras.
type Device struct {
	DevEUI  string `json:"devEUI"`
	DevAddr string `json:"devAddr"`
}

// newSupabaseClient devolve nil (sem erro) quando as credenciais não estão
// configuradas. Isso é proposital: o cadastro é opcional para a API subir — as
// rotas de histórico e de cache continuam funcionando sem ele, e só as que
// dependem do cadastro respondem 503.
func newSupabaseClient() (*supabase.Client, error) {
	url := os.Getenv("SUPABASE_URL")
	key := os.Getenv("SUPABASE_KEY")
	if url == "" || key == "" {
		return nil, nil
	}
	return supabase.NewClient(url, key, nil)
}

// fetchDevicesForUser lista os sensores de uma fazenda.
//
// É a consulta inversa da que o mqtt-sub faz na ingestão: lá se pergunta "de
// quem é este devEUI?" (filtro `cs` sobre a coluna devices); aqui se pergunta
// "quais sensores são deste usuário?". Mesma tabela, mesma fonte de verdade.
//
// O segundo retorno distingue "não há linha para este usuário" de "há linha
// com a lista vazia". Os dois casos devolvem []Device{}, mas só o primeiro
// precisa de provisionamento — ver ensureUserRow.
func fetchDevicesForUser(client *supabase.Client, userId string) ([]Device, bool, error) {
	var rows []struct {
		Devices json.RawMessage `json:"devices"`
	}

	_, err := client.From("users").
		Select("devices", "exact", false).
		Filter("userId", "eq", userId).
		ExecuteTo(&rows)
	if err != nil {
		return nil, false, fmt.Errorf("consulta ao Supabase falhou: %w", err)
	}
	if len(rows) == 0 {
		return []Device{}, false, nil
	}

	devices, err := parseDevices(rows[0].Devices)
	if err != nil {
		return nil, true, err
	}
	return devices, true, nil
}

// uniqueViolationCode é o SQLSTATE de violação de unicidade. O postgrest-go
// formata o erro do PostgREST como "(<código>) <mensagem>", então dá para
// reconhecê-lo sem depender do texto da mensagem nem do nome da constraint.
const uniqueViolationCode = "(23505)"

// ensureUserRow cria a linha do usuário no cadastro, com a lista de sensores
// vazia.
//
// Nome e e-mail ficam de fora de propósito: quem é dono deles é o Auth0 (o
// front lê e grava via Management API), e copiá-los para cá criaria duas
// fontes de verdade divergindo assim que a pessoa editasse o perfil.
//
// Usa Insert, nunca Upsert: com o payload completo o Upsert sobrescreveria a
// coluna `devices`, zerando os sensores de quem já os tem a cada requisição.
// O Insert simples nunca toca em linha existente.
func ensureUserRow(client *supabase.Client, userId string) error {
	payload := map[string]any{
		"userId": userId,
		// Objeto vazio: mesma forma que o mqtt-sub consulta com `cs`.
		"devices": map[string]string{},
	}

	// upsert=false. O "representation" não é enfeite: a issue #51 do
	// postgrest-go relata Insert que devolve sucesso sem gravar nada, e
	// pedir a linha de volta é o que transforma esse silêncio em erro.
	body, _, err := client.From("users").
		Insert(payload, false, "", "representation", "").
		Execute()
	if err != nil {
		// Corrida entre duas requisições da mesma conta: a outra chegou
		// primeiro e criou a linha, que é justamente o resultado desejado.
		if strings.Contains(err.Error(), uniqueViolationCode) {
			return nil
		}
		return fmt.Errorf("cadastro do usuário no Supabase falhou: %w", err)
	}

	var inserted []json.RawMessage
	if err := json.Unmarshal(body, &inserted); err != nil {
		return fmt.Errorf("resposta inesperada ao cadastrar o usuário: %s", string(body))
	}
	if len(inserted) == 0 {
		return fmt.Errorf("cadastro do usuário não teve efeito (nenhuma linha retornada) — ver issue #51 do postgrest-go")
	}
	return nil
}

// devicesForUserEnsuringRow lê os sensores do usuário e, na primeira vez que
// ele aparece, cria sua linha no cadastro.
//
// É provisionamento preguiçoso dentro de um GET: idempotente e sem efeito
// sobre o corpo da resposta. Depois da primeira requisição a linha existe, e
// o custo permanente é zero — a consulta já acontecia.
//
// Falha de escrita não derruba a leitura: o painel de um usuário novo mostra
// "sem sensores" em vez de tela de erro. Mas vai para o log, porque significa
// que o cadastro não está acontecendo.
func devicesForUserEnsuringRow(client *supabase.Client, userId string) ([]Device, error) {
	devices, found, err := fetchDevicesForUser(client, userId)
	if err != nil {
		return nil, err
	}

	if !found {
		if err := ensureUserRow(client, userId); err != nil {
			log.Printf("%v", err)
		}
	}

	return devices, nil
}

// parseDevices tolera as duas formas plausíveis da coluna `devices`.
//
// O mqtt-sub filtra com `cs` sobre o literal {"<devEUI>":"<devAddr>"}, o que
// indica um objeto JSONB mapeando devEUI -> devAddr. Como não foi possível
// inspecionar a tabela real, também aceitamos um array de objetos — se o
// formato for outro, o erro diz exatamente o que veio, em vez de devolver
// lista vazia silenciosamente.
func parseDevices(raw json.RawMessage) ([]Device, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return []Device{}, nil
	}

	// Forma principal: objeto devEUI -> devAddr
	var asMap map[string]string
	if err := json.Unmarshal(raw, &asMap); err == nil {
		devices := make([]Device, 0, len(asMap))
		for devEUI, devAddr := range asMap {
			devices = append(devices, Device{DevEUI: devEUI, DevAddr: devAddr})
		}
		// Ordem estável: sem isso a lista muda de ordem a cada request,
		// porque a iteração de map em Go é aleatória.
		sort.Slice(devices, func(i, j int) bool { return devices[i].DevEUI < devices[j].DevEUI })
		return devices, nil
	}

	// Forma alternativa: array de objetos
	var asList []Device
	if err := json.Unmarshal(raw, &asList); err == nil {
		return asList, nil
	}

	return nil, fmt.Errorf("formato inesperado da coluna devices: %s", string(raw))
}
