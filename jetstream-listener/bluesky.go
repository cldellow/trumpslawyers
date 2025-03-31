package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type PostInfo struct {
	URL         string
	DID         string
	Handle      string
	DisplayName string
	Avatar      string
	CreatedAt   string
	Replies     int
	Reposts     int
	Likes       int
	Quotes      int
	JSON        string
}

func GetBlueskyPost(uri string) (*PostInfo, error) {
	baseURL := "https://public.api.bsky.app/xrpc/app.bsky.feed.getPosts"

	// Build query string: ?uris=<uri>
	reqURL, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("error parsing URL: %w", err)
	}
	q := reqURL.Query()
	q.Add("uris", uri)
	reqURL.RawQuery = q.Encode()

	// Make GET request
	resp, err := http.Get(reqURL.String())
	if err != nil {
		return nil, fmt.Errorf("error making GET request: %w", err)
	}
	defer resp.Body.Close()

	// Read response
	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %w", err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("non-200 response: %s", string(respData))
	}

	// Parse JSON response
	var raw struct {
		Posts []struct {
			URI        string `json:"uri"`
			Author     struct {
				DID         string `json:"did"`
				Handle      string `json:"handle"`
				DisplayName string `json:"displayName"`
				Avatar      string `json:"avatar"`
			} `json:"author"`
			Record struct {
				CreatedAt string `json:"createdAt"`
			} `json:"record"`
			ReplyCount  int `json:"replyCount"`
			RepostCount int `json:"repostCount"`
			LikeCount   int `json:"likeCount"`
			QuoteCount  int `json:"quoteCount"`
		} `json:"posts"`
	}

	if err := json.Unmarshal(respData, &raw); err != nil {
		return nil, fmt.Errorf("error unmarshaling response: %w", err)
	}

	if len(raw.Posts) == 0 {
		return nil, nil // No post found
	}

	post := raw.Posts[0]
	return &PostInfo{
		URL:         post.URI,
		DID:         post.Author.DID,
		Handle:      post.Author.Handle,
		DisplayName: post.Author.DisplayName,
		Avatar:      post.Author.Avatar,
		CreatedAt:   post.Record.CreatedAt,
		Replies:     post.ReplyCount,
		Reposts:     post.RepostCount,
		Likes:       post.LikeCount,
		Quotes:      post.QuoteCount,
		JSON:        string(respData),
	}, nil
}
