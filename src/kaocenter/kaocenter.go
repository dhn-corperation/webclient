package kaocenter

import (
	"bytes"
	"fmt"
	"goclient_knou/src/config"
	"io"

	//db "kaodatabasepool"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	s "strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

var centerClient *http.Client = &http.Client{
	Timeout: time.Second * 30,
	Transport: &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
	},
}

var CENTER_SERVER string = "http://210.114.225.54:8080/"

func FT_Upload(c *gin.Context) {
	file, err := c.FormFile("image")
	if err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("get form err: %s", err.Error()))
		return
	}

	extension := filepath.Ext(file.Filename)
	newFileName := uuid.New().String() + extension

	err = c.SaveUploadedFile(file, "/root/DHNClient/upload/"+newFileName)
	if err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("get form err: %s", err.Error()))
		return
	}

	param := map[string]io.Reader{
		"image": mustOpen("/root/DHNClient/upload/" + newFileName),
	}

	resp, err := upload(CENTER_SERVER+"ft/image", param)

	bytes, _ := io.ReadAll(resp.Body)
	c.Data(http.StatusOK, "application/json", bytes)
}

func FT_Wide_Upload(c *gin.Context) {
	file, err := c.FormFile("image")
	if err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("get form err: %s", err.Error()))
		return
	}

	extension := filepath.Ext(file.Filename)
	newFileName := uuid.New().String() + extension

	err = c.SaveUploadedFile(file, "/root/DHNClient/upload/"+newFileName)
	if err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("get form err: %s", err.Error()))
		return
	}

	param := map[string]io.Reader{
		"image": mustOpen("/root/DHNClient/upload/" + newFileName),
	}

	resp, err := upload(CENTER_SERVER+"ft/wide/image", param)

	bytes, _ := io.ReadAll(resp.Body)
	c.Data(http.StatusOK, "application/json", bytes)
}

func AT_Image(c *gin.Context) {
	file, err := c.FormFile("image")
	if err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("get form err: %s", err.Error()))
		return
	}

	extension := filepath.Ext(file.Filename)
	newFileName := uuid.New().String() + extension

	err = c.SaveUploadedFile(file, "/root/DHNClient/upload/"+newFileName)
	if err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("get form err: %s", err.Error()))
		return
	}

	param := map[string]io.Reader{
		"image": mustOpen("/root/DHNClient/upload/" + newFileName),
	}

	resp, err := upload(CENTER_SERVER+"at/image", param)

	bytes, _ := io.ReadAll(resp.Body)
	c.Data(http.StatusOK, "application/json", bytes)
}

func MMS_Image(c *gin.Context) {
	//conf := config.Conf
	var newFileName1, newFileName2, newFileName3 string

	userID := c.PostForm("userid")
	file1, err1 := c.FormFile("image1")

	if err1 != nil {
		config.Stdlog.Println("File 1 Parameter error : ", err1)
	} else {
		extension1 := filepath.Ext(file1.Filename)
		newFileName1 = "/root/DHNClient/upload/mms/" + uuid.New().String() + extension1

		err := c.SaveUploadedFile(file1, newFileName1)
		if err != nil {
			config.Stdlog.Println("File 1 save error : ", newFileName1, err)
			newFileName1 = ""
		}
	}

	file2, err2 := c.FormFile("image2")

	if err2 != nil {
		config.Stdlog.Println("File 2 Parameter error : ", err2)
	} else {
		extension2 := filepath.Ext(file2.Filename)
		newFileName2 = "/root/DHNClient/upload/mms/" + uuid.New().String() + extension2

		err := c.SaveUploadedFile(file2, newFileName2)
		if err != nil {
			config.Stdlog.Println("File 2 save error : ", newFileName2, err)
			newFileName2 = ""
		}
	}

	file3, err3 := c.FormFile("image3")

	if err3 != nil {
		config.Stdlog.Println("File 3 Parameter error : ", err3)
	} else {
		extension3 := filepath.Ext(file3.Filename)
		newFileName3 = "/root/DHNClient/upload/mms/" + uuid.New().String() + extension3

		err := c.SaveUploadedFile(file3, newFileName3)
		if err != nil {
			config.Stdlog.Println("File 3 save error : ", newFileName3, err)
			newFileName3 = ""
		}
	}

	if len(newFileName1) > 0 || len(newFileName2) > 0 || len(newFileName2) > 0 {

		param := map[string]io.Reader{
			"userid": s.NewReader(userID),
		}
		if len(newFileName1) > 0 {
			param["image1"] = mustOpen(newFileName1)
		}

		if len(newFileName2) > 0 {
			param["image2"] = mustOpen(newFileName2)
		}

		if len(newFileName3) > 0 {
			param["image3"] = mustOpen(newFileName3)
		}

		resp, err := upload(CENTER_SERVER+"mms/image", param)

		if err != nil {
			fmt.Println(err)
		} else {
			bytes, _ := io.ReadAll(resp.Body)
			c.Data(http.StatusOK, "application/json", bytes)
		}
	} else {
		c.JSON(http.StatusNoContent, gin.H{
			"message": "Error",
		})
	}
}

func upload(url string, values map[string]io.Reader) (*http.Response, error) {

	var buff bytes.Buffer
	w := multipart.NewWriter(&buff)

	for key, r := range values {
		var fw io.Writer
		if x, ok := r.(io.Closer); ok {
			defer x.Close()
		}

		if x, ok := r.(*os.File); ok {
			fw, _ = w.CreateFormFile(key, x.Name())
		} else {

			fw, _ = w.CreateFormField(key)
		}
		_, err := io.Copy(fw, r)

		if err != nil {
			return nil, err
		}

	}

	w.Close()

	req, err := http.NewRequest("POST", url, &buff)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", w.FormDataContentType())
	//client := &http.Client{}

	resp, err := centerClient.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func mustOpen(f string) *os.File {
	r, err := os.Open(f)
	if err != nil {
		pwd, _ := os.Getwd()
		fmt.Println("PWD: ", pwd)
		return nil
	}
	return r
}
