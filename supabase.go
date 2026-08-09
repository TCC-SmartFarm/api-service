package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

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
func fetchDevicesForUser(client *supabase.Client, userId string) ([]Device, error) {
	var rows []struct {
		Devices json.RawMessage `json:"devices"`
	}

	_, err := client.From("users").
		Select("devices", "exact", false).
		Filter("userId", "eq", userId).
		ExecuteTo(&rows)
	if err != nil {
		return nil, fmt.Errorf("consulta ao Supabase falhou: %w", err)
	}
	if len(rows) == 0 {
		return []Device{}, nil
	}

	return parseDevices(rows[0].Devices)
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
